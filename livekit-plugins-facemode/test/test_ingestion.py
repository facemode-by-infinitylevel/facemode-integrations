import asyncio
import json
import unittest
from unittest.mock import AsyncMock, patch

from livekit import rtc

from livekit_plugins_facemode import avatar as avatar_module
from livekit_plugins_facemode.avatar import AvatarSession
from livekit_plugins_facemode.exceptions import (
    FaceModeAPIError,
    FaceModeError,
    FaceModeNotReadyError,
    FaceModeProtocolError,
)
from livekit_plugins_facemode.models import (
    LiveKitRoom,
    SessionDetails,
    SessionRequest,
)

_REAL_SLEEP = asyncio.sleep
_WS_END = object()


class _Response:
    def __init__(self, body, status=200):
        self._body = body
        self.status = status

    async def text(self):
        return self._body


class _Request:
    def __init__(self, response):
        self._response = response

    async def __aenter__(self):
        return self._response

    async def __aexit__(self, *_args):
        return False


class _Client:
    def __init__(self, response):
        self._response = response
        self.get_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def get(self, *args, **kwargs):
        self.get_calls.append((args, kwargs))
        return _Request(self._response)


class _SequencedClient(_Client):
    def __init__(self, responses):
        super().__init__(None)
        self._responses = iter(responses)

    def get(self, *args, **kwargs):
        self.get_calls.append((args, kwargs))
        return _Request(next(self._responses))


class _PostClient:
    """Fake aiohttp session that records POSTs and replays queued responses."""

    def __init__(self, responses):
        self._responses = list(responses)
        self.posts = []
        self.get_calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def post(self, url, json=None, headers=None, **_kwargs):
        self.posts.append({"url": url, "json": json, "headers": headers})
        response = self._responses.pop(0) if self._responses else _Response("{}", status=500)
        return _Request(response)

    def get(self, *args, **kwargs):
        self.get_calls.append((args, kwargs))
        return _Request(_Response('{"sessionId":"session-123","ingestion":{"ready":false}}'))


class _FakeWebSocket:
    """Wire-level fake for the ingestion socket.

    Records every sent payload, echoes nothing, and answers ``start`` with a
    canonical ``started`` message unless ``auto_started`` is disabled. Tests
    drive the receive side through ``push``/``drop``/``close``.
    """

    def __init__(self, session_id="session-123", auto_started=True):
        self.session_id = session_id
        self.auto_started = auto_started
        self.sent = []
        self.subprotocol = None
        self.closed = False
        self.close_code = None
        self._queue = asyncio.Queue()

    async def send(self, payload):
        if self.closed:
            raise RuntimeError("send on closed fake socket")
        self.sent.append(payload)
        if isinstance(payload, (bytes, bytearray)):
            return
        message = json.loads(payload)
        if message.get("type") == "start" and self.auto_started:
            self._queue.put_nowait(json.dumps({
                "type": "started",
                "session_id": self.session_id,
                "server_sample_rate": 48000,
                "server_channels": 1,
            }))

    async def close(self):
        self.closed = True
        self._queue.put_nowait(_WS_END)

    def push(self, message):
        self._queue.put_nowait(message if isinstance(message, str) else json.dumps(message))

    def drop(self, error=None):
        """Simulate transport loss: a clean close by default or an error."""
        self.closed = True
        self._queue.put_nowait(error if error is not None else _WS_END)

    def text_messages(self):
        return [json.loads(item) for item in self.sent if isinstance(item, str)]

    def binary_messages(self):
        return [item for item in self.sent if isinstance(item, (bytes, bytearray))]

    def __aiter__(self):
        return self._iterate()

    async def _iterate(self):
        while True:
            item = await self._queue.get()
            if item is _WS_END:
                return
            if isinstance(item, BaseException):
                raise item
            yield item


def _fake_connect(sockets, calls):
    """Build a websockets.connect replacement; echoes the offered subprotocol."""
    queue = list(sockets)

    async def connect(url, **kwargs):
        calls.append({"url": url, "kwargs": kwargs})
        websocket = queue.pop(0)
        offered = kwargs.get("subprotocols") or []
        websocket.subprotocol = offered[0] if offered else None
        return websocket

    return connect


async def _until(predicate, timeout=5.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not predicate():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError("timed out waiting for condition")
        await _REAL_SLEEP(0.005)


class LiveKitIngestionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.room = LiveKitRoom(
            url="wss://livekit.example.test",
            token="worker-room-token",
            name="room-123",
        )

    def _avatar_without_base_initialization(self):
        avatar = object.__new__(AvatarSession)
        avatar.api_key = "api-key"
        avatar.api_url = "https://api.example.test/api"
        return avatar

    def test_pending_response_keeps_initial_room_on_ready_poll(self):
        pending = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "ingestion": {"ready": False, "authority": "websocket"},
            }
        )
        ready = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "workerStatus": "ASSIGNED",
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "one-time-token",
                    "headers": {
                        "X-Runpod-Worker-Id": "strict worker-123",
                        "ignored": 1,
                    },
                },
            },
            fallback=pending,
        )

        self.assertEqual(ready.room, self.room)
        self.assertTrue(ready.ingestion.ready)
        self.assertEqual(
            dict(ready.ingestion.headers),
            {"X-Runpod-Worker-Id": "strict worker-123"},
        )

    def test_pending_response_keeps_initial_room_on_redacted_ready_poll(self):
        pending = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "ingestion": {"ready": False, "authority": "websocket"},
            }
        )
        ready = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": {
                    "type": "livekit",
                    "url": None,
                    "token": None,
                    "name": "room-123",
                },
                "workerStatus": "ASSIGNED",
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "one-time-token",
                },
            },
            fallback=pending,
        )

        self.assertEqual(ready.room, self.room)
        self.assertTrue(ready.ingestion.ready)

    async def test_websocket_connect_forwards_affinity_headers(self):
        avatar = self._avatar_without_base_initialization()
        avatar.session = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "one-time-token",
                    "headers": {"X-Runpod-Worker-Id": "strict worker-123"},
                },
            }
        )
        connect = AsyncMock(return_value=object())

        with patch("livekit_plugins_facemode.avatar.websockets.connect", connect):
            await avatar._connect_websocket()

        connect.assert_awaited_once_with(
            "wss://worker.example.test/ws/session-123",
            subprotocols=["facemode.one-time-token"],
            additional_headers={"X-Runpod-Worker-Id": "strict worker-123"},
            max_size=2**20,
            ping_interval=None,
            compression=None,
            open_timeout=240.0,
        )

    async def test_pending_poll_reaches_ready_assignment_with_room_fallback(self):
        avatar = self._avatar_without_base_initialization()
        pending = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "ingestion": {"ready": False, "authority": "control"},
            }
        )
        client = _SequencedClient(
            [
                _Response(
                    '{"sessionId":"session-123","workerStatus":"ASSIGNING",'
                    '"ingestion":{"ready":false,"authority":"control"}}'
                ),
                _Response(
                    '{"sessionId":"session-123","workerStatus":"ASSIGNED",'
                    '"ingestion":{"ready":true,"authority":"control",'
                    '"url":"wss://worker.example.test/ws/session-123",'
                    '"wsToken":"opaque-websocket-token"}}'
                ),
            ]
        )

        with (
            patch("livekit_plugins_facemode.avatar.aiohttp.ClientSession", return_value=client),
            patch("livekit_plugins_facemode.avatar.asyncio.sleep", AsyncMock()),
        ):
            ready = await avatar._wait_for_ingestion(pending)

        self.assertTrue(ready.ingestion.ready)
        self.assertEqual(ready.room, self.room)
        self.assertEqual(ready.ingestion.ws_token, "opaque-websocket-token")

    async def test_pending_poll_rejects_terminal_worker_states(self):
        for status in ("FAILED", "ENDED"):
            with self.subTest(status=status):
                avatar = self._avatar_without_base_initialization()
                pending = SessionDetails.from_api(
                    {
                        "sessionId": "session-123",
                        "room": self.room.to_payload(),
                        "ingestion": {"ready": False, "authority": "websocket"},
                    }
                )
                client = _Client(
                    _Response(
                        '{"sessionId":"session-123","workerStatus":"'
                        + status
                        + '","ingestion":{"ready":false,"authority":"websocket"}}'
                    )
                )

                with (
                    patch("livekit_plugins_facemode.avatar.aiohttp.ClientSession", return_value=client),
                    patch("livekit_plugins_facemode.avatar.asyncio.sleep", AsyncMock()),
                ):
                    with self.assertRaisesRegex(FaceModeAPIError, f"{status.lower()} state"):
                        await avatar._wait_for_ingestion(pending)

    def test_ingestion_budgets_are_240_seconds(self):
        self.assertEqual(avatar_module._INGESTION_READY_TIMEOUT_SECONDS, 240.0)
        self.assertEqual(avatar_module._WS_OPEN_TIMEOUT_SECONDS, 240.0)

    async def test_immediate_ready_session_skips_polling(self):
        avatar = self._avatar_without_base_initialization()
        ready = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "workerStatus": "ACTIVE",
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "one-time-token",
                },
            }
        )

        def forbidden_client(*_args, **_kwargs):
            raise AssertionError("a ready create response must not open an HTTP session")

        with patch(
            "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
            side_effect=forbidden_client,
        ):
            result = await avatar._wait_for_ingestion(ready)

        self.assertIs(result, ready)

    async def test_initial_terminal_worker_state_short_circuits(self):
        for status in ("FAILED", "ENDED"):
            with self.subTest(status=status):
                avatar = self._avatar_without_base_initialization()
                terminal = SessionDetails.from_api(
                    {
                        "sessionId": "session-123",
                        "room": self.room.to_payload(),
                        "workerStatus": status,
                        "ingestion": {"ready": False, "authority": "websocket"},
                    }
                )
                client = _Client(_Response("{}"))
                sleep = AsyncMock()

                with (
                    patch(
                        "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                        return_value=client,
                    ),
                    patch("livekit_plugins_facemode.avatar.asyncio.sleep", sleep),
                ):
                    with self.assertRaisesRegex(FaceModeAPIError, f"{status.lower()} state"):
                        await avatar._wait_for_ingestion(terminal)

                self.assertEqual(client.get_calls, [])
                sleep.assert_not_called()

    def test_create_payload_omits_wait_for_ingestion(self):
        payload = SessionRequest(
            avatar_id="avatar-1", room=self.room, room_name="room-123"
        ).to_payload()

        self.assertNotIn("waitForIngestion", payload)
        self.assertNotIn("inputProvider", payload)
        self.assertEqual(payload["avatarId"], "avatar-1")
        self.assertEqual(
            payload["room"],
            {
                "type": "livekit",
                "url": "wss://livekit.example.test",
                "token": "worker-room-token",
                "name": "room-123",
            },
        )
        self.assertEqual(payload["livekit_room_id"], "room-123")

    def test_create_payload_includes_input_provider_when_set(self):
        payload = SessionRequest(
            avatar_id="avatar-1",
            room=self.room,
            input_provider="cartesia",
        ).to_payload()

        self.assertEqual(payload["inputProvider"], "cartesia")
        self.assertNotIn("waitForIngestion", payload)

    def test_input_provider_allow_list(self):
        for provider in (
            "deepgram",
            "gemini",
            "gnani",
            "elevenlabs",
            "openai",
            "cartesia",
            "sarvam",
            "custom",
        ):
            with self.subTest(provider=provider):
                request = SessionRequest(
                    avatar_id="avatar-1", room=self.room, input_provider=provider
                )
                self.assertEqual(request.to_payload()["inputProvider"], provider)

        with self.assertRaises(ValueError):
            SessionRequest(avatar_id="a", room=self.room, input_provider="twilio")
        with self.assertRaises(ValueError):
            AvatarSession(api_key="api-key", input_provider="retell")

    def test_session_details_reads_input_provider(self):
        details = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "inputProvider": "gemini",
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "one-time-token",
                },
            }
        )

        self.assertEqual(details.input_provider, "gemini")


class LiveKitReconnectTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.room = LiveKitRoom(
            url="wss://livekit.example.test",
            token="worker-room-token",
            name="room-123",
        )

    def _running_avatar(self, *, auto_started=True):
        """A session that already negotiated the canonical protocol once."""
        avatar = AvatarSession(
            api_key="api-key",
            avatar_id="avatar-1",
            api_url="https://api.example.test/api",
        )
        avatar.session = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "workerStatus": "ACTIVE",
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "token-1",
                },
            }
        )
        avatar._websocket = _FakeWebSocket(auto_started=auto_started)
        avatar._protocol_started.set()
        avatar._protocol_response.set()
        avatar._audio_sample_rate = 16000
        avatar._audio_channels = 1
        avatar._session_log_marker = "session-123"
        return avatar

    def _reconnect_response(self, token):
        return _Response(
            json.dumps(
                {
                    "workerStatus": "ACTIVE",
                    "ingestion": {
                        "ready": True,
                        "url": "wss://worker.example.test/ws/session-123",
                        "wsToken": token,
                        "headers": {"X-Runpod-Worker-Id": "strict worker-123"},
                    },
                }
            )
        )

    async def test_reconnect_request_posts_room_only(self):
        avatar = self._running_avatar()
        client = _PostClient([self._reconnect_response("token-2")])

        with patch(
            "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
            return_value=client,
        ):
            refreshed = await avatar._request_reconnect_session()

        self.assertEqual(len(client.posts), 1)
        post = client.posts[0]
        self.assertEqual(
            post["url"],
            "https://api.example.test/api/sessions/session-123/reconnect",
        )
        self.assertEqual(post["headers"]["Authorization"], "Bearer api-key")
        self.assertEqual(post["json"], {"room": self.room.to_payload()})
        self.assertTrue(refreshed.ingestion.ready)
        self.assertEqual(refreshed.ingestion.ws_token, "token-2")
        self.assertNotEqual(refreshed.ingestion.ws_token, "token-1")

    async def test_receiver_reconnects_on_transport_loss_and_renegotiates(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        socket2 = _FakeWebSocket()
        connect_calls = []
        client = _PostClient([self._reconnect_response("token-2")])

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect",
                _fake_connect([socket2], connect_calls),
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            await _until(
                lambda: avatar._websocket is socket2 and avatar._protocol_started.is_set()
            )
            await avatar._reconnect_task

            socket2.push({"type": "ended"})
            socket2.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(len(client.posts), 1)
        self.assertEqual(len(connect_calls), 1)
        self.assertEqual(
            connect_calls[0]["kwargs"]["subprotocols"], ["facemode.token-2"]
        )
        self.assertEqual(socket2.subprotocol, "facemode.token-2")
        start_messages = [
            message for message in socket2.text_messages() if message["type"] == "start"
        ]
        self.assertEqual(len(start_messages), 1)
        self.assertEqual(
            start_messages[0],
            {
                "type": "start",
                "session_id": "session-123",
                "audio_encoding": "pcm_s16le",
                "sample_rate": 16000,
                "channels": 1,
                "avatar_id": "avatar-1",
                "metadata": {"source": "livekit-agents"},
            },
        )
        self.assertIsNone(avatar._protocol_error)
        self.assertTrue(avatar._session_ended.is_set())

    async def test_simultaneous_failure_signals_trigger_single_reconnect(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        socket2 = _FakeWebSocket()
        client = _PostClient([self._reconnect_response("token-2")])

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect",
                _fake_connect([socket2], []),
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            avatar._start_reconnect()
            avatar._start_reconnect()
            first_task = avatar._reconnect_task
            await _until(lambda: avatar._protocol_started.is_set() and avatar._websocket is socket2)
            await first_task

            socket2.push({"type": "ended"})
            socket2.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertIs(avatar._reconnect_task, first_task)
        self.assertEqual(len(client.posts), 1)

    async def test_each_reconnect_uses_a_unique_fresh_token(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        socket2 = _FakeWebSocket()
        socket3 = _FakeWebSocket()
        connect_calls = []
        client = _PostClient(
            [self._reconnect_response("token-2"), self._reconnect_response("token-3")]
        )

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect",
                _fake_connect([socket2, socket3], connect_calls),
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            await _until(lambda: avatar._websocket is socket2 and avatar._protocol_started.is_set())
            socket2.drop()
            await _until(lambda: avatar._websocket is socket3 and avatar._protocol_started.is_set())

            socket3.push({"type": "ended"})
            socket3.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(len(connect_calls), 2)
        used = [call["kwargs"]["subprotocols"][0] for call in connect_calls]
        self.assertEqual(used, ["facemode.token-2", "facemode.token-3"])
        self.assertNotIn("facemode.token-1", used)
        self.assertEqual(len(set(used)), 2)

    async def test_send_barrier_waits_until_restarted(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        socket2 = _FakeWebSocket(auto_started=False)
        client = _PostClient([self._reconnect_response("token-2")])

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect",
                _fake_connect([socket2], []),
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            await _until(lambda: any(m["type"] == "start" for m in socket2.text_messages()))

            sender = asyncio.create_task(avatar._send_control("end_utterance", 9))
            await _REAL_SLEEP(0.05)
            self.assertFalse(sender.done())
            self.assertNotIn(
                "end_utterance",
                [m["type"] for m in socket2.text_messages()],
            )

            socket2.push(
                {
                    "type": "started",
                    "session_id": "session-123",
                    "server_sample_rate": 48000,
                    "server_channels": 1,
                }
            )
            await asyncio.wait_for(sender, timeout=5)

            socket2.push({"type": "ended"})
            socket2.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertIn(
            {"type": "end_utterance", "seq": 9},
            socket2.text_messages(),
        )

    async def test_sequence_numbers_stay_monotonic_across_reconnect(self):
        avatar = self._running_avatar()
        avatar._sequence = 7
        socket1 = avatar._websocket
        socket2 = _FakeWebSocket()
        client = _PostClient([self._reconnect_response("token-2")])

        await avatar._send_control("start_utterance", avatar._next_sequence())

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect",
                _fake_connect([socket2], []),
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            await _until(lambda: avatar._websocket is socket2 and avatar._protocol_started.is_set())
            await avatar._reconnect_task
            await avatar._send_control("end_utterance", avatar._next_sequence())

            socket2.push({"type": "ended"})
            socket2.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertIn({"type": "start_utterance", "seq": 7}, socket1.text_messages())
        self.assertIn({"type": "end_utterance", "seq": 8}, socket2.text_messages())
        start_message = next(
            m for m in socket2.text_messages() if m["type"] == "start"
        )
        self.assertNotIn("seq", start_message)

    async def test_audio_sent_before_drop_is_not_replayed(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        socket2 = _FakeWebSocket()
        client = _PostClient([self._reconnect_response("token-2")])
        frame_a = rtc.AudioFrame(
            data=b"\x01\x00" * 160,
            sample_rate=16000,
            num_channels=1,
            samples_per_channel=160,
        )
        frame_b = rtc.AudioFrame(
            data=b"\x02\x00" * 160,
            sample_rate=16000,
            num_channels=1,
            samples_per_channel=160,
        )

        await avatar._send_tts_audio(frame_a)

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect",
                _fake_connect([socket2], []),
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            await _until(lambda: avatar._websocket is socket2 and avatar._protocol_started.is_set())
            await avatar._reconnect_task
            await avatar._send_tts_audio(frame_b)

            socket2.push({"type": "ended"})
            socket2.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(socket1.binary_messages(), [frame_a.data.tobytes()])
        self.assertEqual(socket2.binary_messages(), [frame_b.data.tobytes()])

    async def test_no_reconnect_when_stopped(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        client = _PostClient([self._reconnect_response("token-2")])
        connect = AsyncMock()

        with (
            patch("livekit_plugins_facemode.avatar.websockets.connect", connect),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            avatar._stopped = True
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertIsNone(avatar._reconnect_task)
        self.assertEqual(client.posts, [])
        connect.assert_not_called()
        self.assertIsNone(avatar._protocol_error)

    async def test_no_reconnect_on_fatal_protocol_error(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        client = _PostClient([self._reconnect_response("token-2")])
        connect = AsyncMock()

        with (
            patch("livekit_plugins_facemode.avatar.websockets.connect", connect),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.push(
                {"type": "error", "code": "SESSION_GONE", "message": "gone", "fatal": True}
            )
            socket1.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertIsNotNone(avatar._protocol_error)
        self.assertIsNone(avatar._reconnect_task)
        self.assertEqual(client.posts, [])
        connect.assert_not_called()

    async def test_no_reconnect_after_session_ended(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        client = _PostClient([self._reconnect_response("token-2")])
        connect = AsyncMock()

        with (
            patch("livekit_plugins_facemode.avatar.websockets.connect", connect),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.push({"type": "ended"})
            socket1.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertTrue(avatar._session_ended.is_set())
        self.assertIsNone(avatar._reconnect_task)
        self.assertEqual(client.posts, [])
        connect.assert_not_called()

    async def test_no_reconnect_when_socket_dies_before_first_start(self):
        avatar = self._running_avatar()
        avatar._protocol_started.clear()
        avatar._protocol_response.clear()
        socket1 = avatar._websocket
        client = _PostClient([self._reconnect_response("token-2")])
        connect = AsyncMock()

        with (
            patch("livekit_plugins_facemode.avatar.websockets.connect", connect),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertIsInstance(avatar._protocol_error, FaceModeProtocolError)
        self.assertIsNone(avatar._reconnect_task)
        self.assertEqual(client.posts, [])
        connect.assert_not_called()

    async def test_reconnect_exhaustion_raises_typed_errors(self):
        avatar = self._running_avatar()
        avatar._protocol_started.clear()
        avatar._protocol_response.clear()

        async def refused_connect(*_args, **_kwargs):
            raise ConnectionError("refused")

        client = _PostClient(
            [self._reconnect_response("token-2"), self._reconnect_response("token-3")]
        )

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect", refused_connect
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            await avatar._reconnect()

        self.assertIsInstance(avatar._protocol_error, FaceModeNotReadyError)
        self.assertIsInstance(avatar._protocol_error, FaceModeError)
        self.assertEqual(len(client.posts), 2)

    async def test_reconnect_exhaustion_surfaces_api_error(self):
        avatar = self._running_avatar()
        avatar._protocol_started.clear()
        avatar._protocol_response.clear()
        client = _PostClient(
            [
                _Response('{"error":{"code":"SESSION_NOT_ACTIVE"}}', status=409),
                _Response('{"error":{"code":"SESSION_NOT_ACTIVE"}}', status=409),
            ]
        )
        connect = AsyncMock()

        with (
            patch("livekit_plugins_facemode.avatar.websockets.connect", connect),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            await avatar._reconnect()

        self.assertIsInstance(avatar._protocol_error, FaceModeAPIError)
        self.assertEqual(len(client.posts), 2)
        connect.assert_not_called()

    async def test_cancelled_reconnect_closes_pending_socket(self):
        avatar = self._running_avatar()
        avatar._protocol_started.clear()
        avatar._protocol_response.clear()
        socket2 = _FakeWebSocket(auto_started=False)
        client = _PostClient([self._reconnect_response("token-2")])

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect",
                _fake_connect([socket2], []),
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            task = asyncio.create_task(avatar._reconnect())
            await _until(lambda: avatar._websocket is socket2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(socket2.closed)
        self.assertTrue(task.cancelled())

    async def test_stop_during_reconnect_cancels_task(self):
        avatar = self._running_avatar()
        socket1 = avatar._websocket
        socket2 = _FakeWebSocket(auto_started=False)
        client = _PostClient([self._reconnect_response("token-2")])

        with (
            patch(
                "livekit_plugins_facemode.avatar.websockets.connect",
                _fake_connect([socket2], []),
            ),
            patch(
                "livekit_plugins_facemode.avatar.aiohttp.ClientSession",
                return_value=client,
            ),
        ):
            receiver = asyncio.create_task(avatar._receive_events())
            socket1.drop()
            await _until(lambda: avatar._reconnect_task is not None)
            reconnect_task = avatar._reconnect_task
            await avatar.stop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertTrue(reconnect_task.done())
        self.assertTrue(socket2.closed)


if __name__ == "__main__":
    unittest.main()
