import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pipecat.frames.frames import EndFrame
from pipecat.processors.frame_processor import FrameDirection

from pipecat_facemode.exceptions import (
    FaceModeNotReadyError,
    FaceModeProtocolError,
)
from pipecat_facemode.models import LiveKitRoom, SessionDetails
from pipecat_facemode.service import FaceModeVideoService


_WS_END = object()
_REAL_SLEEP = asyncio.sleep


class _Response:
    def __init__(self, body, status=200):
        self._body = body
        self.status = status

    async def text(self):
        return self._body


class _Request:
    def __init__(self, response, gate=None):
        self._response = response
        self._gate = gate

    async def __aenter__(self):
        if self._gate is not None:
            await self._gate.wait()
        if isinstance(self._response, BaseException):
            raise self._response
        return self._response

    async def __aexit__(self, *_args):
        return False


class _RecordingClient:
    """aiohttp-shaped fake recording calls and replaying queued responses."""

    def __init__(self, *, posts=None, gets=None, post_gate=None):
        self.post_calls = []
        self.get_calls = []
        self._posts = list(posts or [])
        self._gets = list(gets or [])
        self.post_gate = post_gate

    def post(self, url, **kwargs):
        self.post_calls.append({"url": url, **kwargs})
        response = self._posts.pop(0) if self._posts else _Response("{}")
        return _Request(response, gate=self.post_gate)

    def get(self, url, **kwargs):
        self.get_calls.append({"url": url, **kwargs})
        response = self._gets.pop(0) if self._gets else _Response("{}")
        return _Request(response)


class _FakeWebSocket:
    """Queue-driven websocket fake following the async-iterator protocol."""

    def __init__(self):
        self.sent_text = []
        self.sent_binary = []
        self.closed = False
        self.close_code = None
        self._queue = asyncio.Queue()

    def __aiter__(self):
        return self

    async def __anext__(self):
        item = await self._queue.get()
        if item is _WS_END:
            raise StopAsyncIteration
        if isinstance(item, BaseException):
            raise item
        return item

    async def send(self, data):
        if self.closed:
            raise ConnectionError("websocket is closed")
        if isinstance(data, (bytes, bytearray, memoryview)):
            self.sent_binary.append(bytes(data))
        else:
            self.sent_text.append(data)

    async def close(self):
        self.closed = True
        self._queue.put_nowait(_WS_END)

    def feed_json(self, payload):
        self._queue.put_nowait(json.dumps(payload))

    def drop(self, error=None):
        self._queue.put_nowait(error or ConnectionError("transport lost"))

    def server_close(self):
        self._queue.put_nowait(_WS_END)


def _session_payload(token, room):
    return {
        "sessionId": "session-123",
        "workerStatus": "ASSIGNED",
        "room": room.to_payload(),
        "ingestion": {
            "ready": True,
            "authority": "websocket",
            "url": "wss://worker.example.test/ws/session-123",
            "wsToken": token,
        },
    }


def _reconnect_payload(token):
    return json.dumps(
        {
            "workerStatus": "ASSIGNED",
            "inputProvider": "deepgram",
            "ingestion": {
                "ready": True,
                "authority": "websocket",
                "url": "wss://worker.example.test/ws/session-123",
                "wsToken": token,
            },
        }
    )


def _started_message():
    return {
        "type": "started",
        "session_id": "session-123",
        "server_sample_rate": 48_000,
        "server_channels": 1,
    }


def _started_socket():
    websocket = _FakeWebSocket()
    websocket.feed_json(_started_message())
    return websocket


async def _yield(turns=6):
    for _ in range(turns):
        await _REAL_SLEEP(0)


async def _instant_sleep(*_args, **_kwargs):
    """asyncio.sleep replacement that yields once instead of waiting."""
    await _REAL_SLEEP(0)


class ReconnectTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.room = LiveKitRoom(
            url="wss://livekit.example.test",
            token="worker-room-token",
            name="room-123",
        )
        self.service = None
        self.receiver = None

    async def asyncTearDown(self):
        if self.service is not None:
            self.service._stopping = True
            await self.service._shutdown(send_end_session=False)

    def _started_service(self, client, websocket, input_provider="deepgram"):
        service = FaceModeVideoService(
            api_key="api-key",
            room=self.room,
            http_session=client,
            input_provider=input_provider,
            protocol_timeout=5.0,
        )
        service.session = SessionDetails.from_api(
            _session_payload("token-initial", self.room)
        )
        service.websocket = websocket
        service._started = True
        service._protocol_started = True
        service._protocol_start_sent = True
        service._protocol_started_event.set()
        service._audio_sample_rate = 24_000
        service._audio_channels = 1
        service._transport_ready_event.set()
        self.service = service
        return service

    async def test_transport_loss_reconnects_with_fresh_token_and_resumes(self):
        ws1 = _FakeWebSocket()
        ws2 = _started_socket()
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-1"))]
        )
        service = self._started_service(client, ws1)
        connect = AsyncMock(return_value=ws2)

        with patch("pipecat_facemode.service.websockets.connect", connect):
            receiver = asyncio.create_task(service._receive_events())
            service._receiver_task = receiver
            ws1.drop()
            await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(len(client.post_calls), 1)
        call = client.post_calls[0]
        self.assertTrue(call["url"].endswith("/sessions/session-123/reconnect"))
        self.assertEqual(call["json"]["room"]["token"], "worker-room-token")
        self.assertEqual(call["json"]["inputProvider"], "deepgram")
        connect.assert_awaited_once()
        self.assertEqual(
            connect.await_args.kwargs["subprotocols"], ["facemode.token-fresh-1"]
        )
        self.assertIs(service.websocket, ws2)
        self.assertTrue(service._protocol_started)
        self.assertIsNone(service._protocol_error)
        self.assertTrue(service._started)

        start = json.loads(ws2.sent_text[0])
        self.assertEqual(start["type"], "start")
        self.assertEqual(start["session_id"], "session-123")
        self.assertEqual(start["sample_rate"], 24_000)
        self.assertEqual(start["channels"], 1)

        await service._send_binary(b"resumed-audio")
        self.assertEqual(ws2.sent_binary, [b"resumed-audio"])

    async def test_concurrent_drops_trigger_single_reconnect(self):
        ws1 = _FakeWebSocket()
        ws2 = _started_socket()
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-2"))]
        )
        service = self._started_service(client, ws1)
        connect = AsyncMock(return_value=ws2)

        with patch("pipecat_facemode.service.websockets.connect", connect):
            results = await asyncio.gather(
                service._maybe_recover_transport(ws1),
                service._maybe_recover_transport(ws1),
            )

        self.assertEqual(results, [True, True])
        self.assertEqual(len(client.post_calls), 1)
        connect.assert_awaited_once()

    async def test_each_attempt_uses_a_unique_token(self):
        ws1 = _FakeWebSocket()
        ws2 = _started_socket()
        client = _RecordingClient(
            posts=[
                _Response(_reconnect_payload("token-attempt-1")),
                _Response(_reconnect_payload("token-attempt-2")),
            ]
        )
        service = self._started_service(client, ws1)
        connect = AsyncMock(side_effect=[ConnectionError("refused"), ws2])

        with patch(
            "pipecat_facemode.service.websockets.connect", connect
        ), patch(
            "pipecat_facemode.service.asyncio.sleep",
            new=_instant_sleep,
        ):
            recovered = await service._maybe_recover_transport(ws1)

        self.assertTrue(recovered)
        self.assertEqual(len(client.post_calls), 2)
        prefixes = [
            call.kwargs["subprotocols"][0] for call in connect.await_args_list
        ]
        self.assertEqual(
            prefixes, ["facemode.token-attempt-1", "facemode.token-attempt-2"]
        )
        self.assertIs(service.websocket, ws2)

    async def test_reconnect_renegotiates_started_and_keeps_utterance(self):
        ws1 = _FakeWebSocket()
        ws2 = _started_socket()
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-3"))]
        )
        service = self._started_service(client, ws1)
        service._utterance_active = True
        service._utterance_context_id = "ctx-1"
        connect = AsyncMock(return_value=ws2)

        with patch("pipecat_facemode.service.websockets.connect", connect):
            receiver = asyncio.create_task(service._receive_events())
            service._receiver_task = receiver
            ws1.drop()
            await asyncio.wait_for(receiver, timeout=5)

        sent = [json.loads(text) for text in ws2.sent_text]
        self.assertEqual(sent[0]["type"], "start")
        # An explicitly open utterance is re-declared on the new socket so
        # following audio still lands inside an utterance.
        self.assertIn("start_utterance", [message["type"] for message in sent])
        self.assertTrue(service._utterance_active)

    async def test_sequence_numbers_stay_monotonic_across_reconnect(self):
        ws1 = _FakeWebSocket()
        ws2 = _started_socket()
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-4"))]
        )
        service = self._started_service(client, ws1)
        connect = AsyncMock(return_value=ws2)

        await service._send_control("end_utterance")
        first = json.loads(ws1.sent_text[-1])
        self.assertEqual(first["seq"], 0)

        with patch("pipecat_facemode.service.websockets.connect", connect):
            recovered = await service._maybe_recover_transport(ws1)
        self.assertTrue(recovered)

        await service._send_control("end_utterance")
        last = json.loads(ws2.sent_text[-1])
        self.assertEqual(last["type"], "end_utterance")
        self.assertEqual(last["seq"], 1)

    async def test_send_waits_for_reconnect_barrier(self):
        ws1 = _FakeWebSocket()
        ws2 = _started_socket()
        gate = asyncio.Event()
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-5"))],
            post_gate=gate,
        )
        service = self._started_service(client, ws1)
        connect = AsyncMock(return_value=ws2)

        with patch("pipecat_facemode.service.websockets.connect", connect):
            receiver = asyncio.create_task(service._receive_events())
            service._receiver_task = receiver
            ws1.drop()
            for _ in range(50):
                await _REAL_SLEEP(0)
                if client.post_calls:
                    break
            self.assertEqual(len(client.post_calls), 1)

            sender = asyncio.create_task(service._send_binary(b"held-audio"))
            await _yield(10)
            self.assertFalse(sender.done())
            self.assertEqual(ws2.sent_binary, [])

            gate.set()
            await asyncio.wait_for(sender, timeout=5)
            await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(ws2.sent_binary, [b"held-audio"])
        first_message = json.loads(ws2.sent_text[0])
        self.assertEqual(first_message["type"], "start")

    async def test_no_reconnect_on_ended(self):
        ws1 = _FakeWebSocket()
        client = _RecordingClient()
        service = self._started_service(client, ws1)

        receiver = asyncio.create_task(service._receive_events())
        service._receiver_task = receiver
        ws1.feed_json({"type": "ended"})
        await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(client.post_calls, [])
        self.assertTrue(service._session_ended_event.is_set())

    async def test_no_reconnect_on_fatal_error(self):
        ws1 = _FakeWebSocket()
        client = _RecordingClient()
        service = self._started_service(client, ws1)

        receiver = asyncio.create_task(service._receive_events())
        service._receiver_task = receiver
        ws1.feed_json(
            {"type": "error", "code": "FATAL", "message": "dead", "fatal": True}
        )
        await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(client.post_calls, [])
        self.assertIsInstance(service._protocol_error, FaceModeProtocolError)
        self.assertIn("FATAL", str(service._protocol_error))

    async def test_no_reconnect_on_session_ending(self):
        ws1 = _FakeWebSocket()
        client = _RecordingClient()
        service = self._started_service(client, ws1)

        receiver = asyncio.create_task(service._receive_events())
        service._receiver_task = receiver
        ws1.feed_json({"type": "session_ending", "reason": "idle"})
        await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(client.post_calls, [])
        self.assertIsInstance(service._protocol_error, FaceModeProtocolError)
        self.assertIn("ending", str(service._protocol_error))

    async def test_no_reconnect_during_intentional_shutdown(self):
        ws1 = _FakeWebSocket()
        client = _RecordingClient()
        service = self._started_service(client, ws1)
        service._stopping = True

        receiver = asyncio.create_task(service._receive_events())
        service._receiver_task = receiver
        ws1.drop()
        await asyncio.wait_for(receiver, timeout=5)

        self.assertEqual(client.post_calls, [])
        self.assertIsNone(service._protocol_error)

    async def test_reconnect_exhaustion_raises_typed_nonleaky_error(self):
        ws1 = _FakeWebSocket()
        client = _RecordingClient(
            posts=[
                ConnectionError("backend down"),
                _Response("{}", status=500),
            ]
        )
        service = self._started_service(client, ws1)

        with patch(
            "pipecat_facemode.service.asyncio.sleep",
            new=_instant_sleep,
        ):
            recovered = await service._maybe_recover_transport(ws1)

        self.assertFalse(recovered)
        self.assertEqual(len(client.post_calls), 2)
        self.assertIsInstance(service._protocol_error, FaceModeProtocolError)
        message = str(service._protocol_error)
        self.assertIn("reconnect failed after 2 attempts", message)
        self.assertNotIn("token-initial", message)
        self.assertNotIn("worker-room-token", message)
        self.assertNotIn("api-key", message)

    async def test_reconnect_cancellation_cleans_up(self):
        ws1 = _FakeWebSocket()
        gate = asyncio.Event()  # never set: the POST blocks until cancelled
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-6"))],
            post_gate=gate,
        )
        service = self._started_service(client, ws1)

        task = asyncio.create_task(service._maybe_recover_transport(ws1))
        for _ in range(50):
            await _REAL_SLEEP(0)
            if client.post_calls:
                break
        self.assertEqual(len(client.post_calls), 1)

        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

        self.assertFalse(service._reconnect_lock.locked())
        self.assertIsNone(service._keepalive_task)
        self.assertIsNone(service.websocket)

    async def test_reconnect_does_not_replay_audio(self):
        ws1 = _FakeWebSocket()
        ws2 = _started_socket()
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-7"))]
        )
        service = self._started_service(client, ws1)
        connect = AsyncMock(return_value=ws2)

        await service._send_binary(b"first-chunk")
        with patch("pipecat_facemode.service.websockets.connect", connect):
            recovered = await service._maybe_recover_transport(ws1)
        self.assertTrue(recovered)
        await service._send_binary(b"second-chunk")

        self.assertEqual(ws1.sent_binary, [b"first-chunk"])
        self.assertEqual(ws2.sent_binary, [b"second-chunk"])

    async def test_reconnect_request_body_is_room_only(self):
        ws1 = _FakeWebSocket()
        ws2 = _started_socket()
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-8"))]
        )
        service = self._started_service(client, ws1, input_provider=None)
        connect = AsyncMock(return_value=ws2)

        with patch("pipecat_facemode.service.websockets.connect", connect):
            recovered = await service._maybe_recover_transport(ws1)

        self.assertTrue(recovered)
        self.assertEqual(len(client.post_calls), 1)
        body = client.post_calls[0]["json"]
        # The strict reconnect endpoint accepts only the original room object
        # (plus inputProvider when one was configured on the service).
        self.assertEqual(set(body.keys()), {"room"})
        self.assertEqual(body["room"], self.room.to_payload())

    async def test_no_reconnect_before_session_started(self):
        ws1 = _FakeWebSocket()
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-unused"))]
        )
        service = FaceModeVideoService(
            api_key="api-key",
            room=self.room,
            http_session=client,
        )
        service.session = SessionDetails.from_api(
            _session_payload("token-initial", self.room)
        )
        service.websocket = ws1
        self.service = service

        # A transport loss before a successful start handshake is not
        # recoverable; the startup path surfaces the original failure.
        recovered = await service._maybe_recover_transport(ws1)

        self.assertFalse(recovered)
        self.assertEqual(client.post_calls, [])

    async def test_reconnect_cancelled_mid_open_closes_socket(self):
        ws1 = _FakeWebSocket()
        ws2 = _FakeWebSocket()  # never sends started; the flight stalls
        client = _RecordingClient(
            posts=[_Response(_reconnect_payload("token-fresh-9"))]
        )
        service = self._started_service(client, ws1)
        connect = AsyncMock(return_value=ws2)

        with patch("pipecat_facemode.service.websockets.connect", connect):
            task = asyncio.create_task(service._maybe_recover_transport(ws1))
            for _ in range(50):
                await _REAL_SLEEP(0)
                if service.websocket is ws2:
                    break
            self.assertIs(service.websocket, ws2)

            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        self.assertTrue(ws2.closed)
        self.assertIsNone(service.websocket)
        self.assertIsNone(service._receiver_task)
        self.assertIsNone(service._keepalive_task)
        self.assertFalse(service._reconnect_lock.locked())

    async def test_start_failure_does_not_deadlock_or_leak(self):
        client = _RecordingClient()
        service = FaceModeVideoService(
            api_key="api-key",
            room=self.room,
            http_session=client,
        )
        self.service = service
        service._connect_livekit = AsyncMock(
            side_effect=FaceModeNotReadyError("no room")
        )
        frame = SimpleNamespace(audio_out_sample_rate=24_000)

        # _start_session holds the lifecycle lock while failing; teardown must
        # still run instead of re-entering _shutdown on the same lock.
        with self.assertRaises(FaceModeNotReadyError):
            await asyncio.wait_for(
                service._start_session(frame), timeout=5
            )

        self.assertFalse(service._started)
        self.assertIsNone(service.session)
        self.assertIsNone(service.room)
        self.assertIsNone(service.websocket)
        self.assertFalse(service._lifecycle_lock.locked())

    async def test_shutdown_without_ended_ack_still_tears_down(self):
        ws1 = _FakeWebSocket()  # accepts end_session but never replies "ended"
        client = _RecordingClient()
        service = self._started_service(client, ws1)
        disconnect_calls = []

        async def _disconnect():
            disconnect_calls.append(True)

        room = SimpleNamespace(disconnect=_disconnect)
        service.room = room
        receiver = asyncio.create_task(service._receive_events())
        service._receiver_task = receiver
        push_frame = AsyncMock()
        service.push_frame = push_frame
        frame = EndFrame()

        # A zero ack budget makes asyncio.wait_for raise TimeoutError
        # immediately; the missing "ended" must not escape or skip teardown.
        with patch(
            "pipecat_facemode.service._END_ACK_TIMEOUT_SECONDS", 0
        ):
            await service.process_frame(frame, FrameDirection.DOWNSTREAM)

        sent = [json.loads(text) for text in ws1.sent_text]
        self.assertIn("end_session", [message["type"] for message in sent])
        self.assertTrue(ws1.closed)
        self.assertTrue(receiver.done())
        self.assertIsNone(service._receiver_task)
        self.assertIsNone(service._keepalive_task)
        self.assertIsNone(service.websocket)
        self.assertIsNone(service.session)
        self.assertIsNone(service.room)
        self.assertEqual(disconnect_calls, [True])
        self.assertFalse(service._started)
        self.assertFalse(service._protocol_started)
        self.assertFalse(service._stopping)
        push_frame.assert_awaited_once_with(frame, FrameDirection.DOWNSTREAM)

    async def test_reconnect_rejects_mismatched_session(self):
        ws1 = _FakeWebSocket()
        wrong_session = json.dumps(
            {
                "sessionId": "session-999",
                "room": self.room.to_payload(),
                "workerStatus": "ASSIGNED",
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-999",
                    "wsToken": "token-wrong",
                },
            }
        )
        client = _RecordingClient(
            posts=[_Response(wrong_session), _Response(wrong_session)]
        )
        service = self._started_service(client, ws1)
        connect = AsyncMock()

        with patch(
            "pipecat_facemode.service.websockets.connect", connect
        ), patch(
            "pipecat_facemode.service.asyncio.sleep",
            new=_instant_sleep,
        ):
            recovered = await service._maybe_recover_transport(ws1)

        self.assertFalse(recovered)
        connect.assert_not_awaited()
        self.assertIsInstance(service._protocol_error, FaceModeProtocolError)


if __name__ == "__main__":
    unittest.main()
