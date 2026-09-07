import unittest
from unittest.mock import AsyncMock, patch

from livekit_plugins_facemode.avatar import AvatarSession
from livekit_plugins_facemode.exceptions import FaceModeAPIError
from livekit_plugins_facemode.models import LiveKitRoom, SessionDetails


class _Response:
    status = 200

    def __init__(self, body):
        self._body = body

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

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    def get(self, *_args, **_kwargs):
        return _Request(self._response)


class _SequencedClient(_Client):
    def __init__(self, responses):
        self._responses = iter(responses)

    def get(self, *_args, **_kwargs):
        return _Request(next(self._responses))


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
            subprotocols=["aivatar.one-time-token"],
            additional_headers={"X-Runpod-Worker-Id": "strict worker-123"},
            max_size=2**20,
            ping_interval=None,
            compression=None,
            open_timeout=60,
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


if __name__ == "__main__":
    unittest.main()
