import unittest
from unittest.mock import AsyncMock, patch

from pipecat_facemode.exceptions import FaceModeAPIError
from pipecat_facemode.models import LiveKitRoom, SessionDetails
from pipecat_facemode.service import FaceModeVideoService


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


class _SequencedClient:
    def __init__(self, responses):
        self._responses = iter(responses)

    def get(self, *_args, **_kwargs):
        return _Request(next(self._responses))


class PipecatIngestionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.room = LiveKitRoom(
            url="wss://livekit.example.test",
            token="worker-room-token",
            name="room-123",
        )

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

    async def test_pending_poll_reaches_ready_assignment_with_room_fallback(self):
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
        service = FaceModeVideoService(
            api_key="api-key",
            room=self.room,
            http_session=client,
        )

        with patch("pipecat_facemode.service.asyncio.sleep", AsyncMock()):
            ready = await service._wait_for_ingestion(pending)

        self.assertTrue(ready.ingestion.ready)
        self.assertEqual(ready.room, self.room)
        self.assertEqual(ready.ingestion.ws_token, "opaque-websocket-token")

    async def test_pending_poll_rejects_terminal_worker_state(self):
        pending = SessionDetails.from_api(
            {
                "sessionId": "session-terminal",
                "room": self.room.to_payload(),
                "workerStatus": "ASSIGNING",
                "ingestion": {"ready": False, "authority": "control"},
            }
        )
        service = FaceModeVideoService(
            api_key="api-key",
            room=self.room,
            http_session=_SequencedClient(
                [
                    _Response(
                        '{"sessionId":"session-terminal","workerStatus":"FAILED",'
                        '"ingestion":{"ready":false,"authority":"control"}}'
                    )
                ]
            ),
        )

        with patch("pipecat_facemode.service.asyncio.sleep", AsyncMock()):
            with self.assertRaisesRegex(
                FaceModeAPIError,
                "FaceMode ingestion worker entered failed state",
            ):
                await service._wait_for_ingestion(pending)

    async def test_websocket_connect_forwards_affinity_without_fallback(self):
        service = FaceModeVideoService(api_key="api-key", room=self.room)
        service.session = SessionDetails.from_api(
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

        with patch("pipecat_facemode.service.websockets.connect", connect):
            await service._connect_websocket()

        connect.assert_awaited_once_with(
            "wss://worker.example.test/ws/session-123",
            subprotocols=["aivatar.one-time-token"],
            additional_headers={"X-Runpod-Worker-Id": "strict worker-123"},
            max_size=2**20,
            ping_interval=None,
            compression=None,
            open_timeout=60,
        )


if __name__ == "__main__":
    unittest.main()
