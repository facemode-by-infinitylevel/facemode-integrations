import json
import unittest
from unittest.mock import AsyncMock, patch

import pipecat_facemode.service as service_module
from pipecat_facemode.exceptions import (
    FaceModeAPIError,
    FaceModeConfigurationError,
)
from pipecat_facemode.models import LiveKitRoom, SessionDetails, SessionRequest
from pipecat_facemode.service import FaceModeVideoService


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
        if isinstance(self._response, BaseException):
            raise self._response
        return self._response

    async def __aexit__(self, *_args):
        return False


class _RecordingClient:
    """Minimal aiohttp-shaped fake that records requests and replays responses."""

    def __init__(self, *, posts=None, gets=None):
        self.post_calls = []
        self.get_calls = []
        self._posts = list(posts or [])
        self._gets = list(gets or [])

    def post(self, url, **kwargs):
        self.post_calls.append({"url": url, **kwargs})
        response = self._posts.pop(0) if self._posts else _Response("{}")
        return _Request(response)

    def get(self, url, **kwargs):
        self.get_calls.append({"url": url, **kwargs})
        response = self._gets.pop(0) if self._gets else _Response("{}")
        return _Request(response)


def _ready_payload(token="one-time-token"):
    return json.dumps(
        {
            "sessionId": "session-123",
            "workerStatus": "ASSIGNED",
            "room": {
                "type": "livekit",
                "url": "wss://livekit.example.test",
                "token": "worker-room-token",
                "name": "room-123",
            },
            "ingestion": {
                "ready": True,
                "url": "wss://worker.example.test/ws/session-123",
                "wsToken": token,
            },
        }
    )


class SessionRequestContractTests(unittest.TestCase):
    def setUp(self):
        self.room = LiveKitRoom(
            url="wss://livekit.example.test",
            token="worker-room-token",
            name="room-123",
        )

    def test_create_payload_drops_wait_for_ingestion(self):
        request = SessionRequest(avatar_id="avatar-1", room=self.room)
        self.assertFalse(hasattr(request, "wait_for_ingestion"))
        payload = request.to_payload()
        self.assertNotIn("waitForIngestion", payload)
        self.assertEqual(payload["avatarId"], "avatar-1")
        self.assertEqual(payload["room"]["url"], "wss://livekit.example.test")
        self.assertNotIn("inputProvider", payload)

    def test_create_payload_sends_optional_input_provider(self):
        request = SessionRequest(
            avatar_id="avatar-1",
            room=self.room,
            input_provider="deepgram",
        )
        self.assertEqual(request.to_payload()["inputProvider"], "deepgram")

    def test_session_details_parse_input_provider(self):
        details = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "inputProvider": "gemini",
                "ingestion": {"ready": False},
            }
        )
        self.assertEqual(details.input_provider, "gemini")

    def test_session_details_parse_flat_ready_envelope(self):
        details = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "ready": True,
                "url": "wss://worker.example.test/ws/session-123",
                "wsToken": "flat-token",
            }
        )
        self.assertTrue(details.ingestion.ready)
        self.assertEqual(details.ingestion.url, "wss://worker.example.test/ws/session-123")
        self.assertEqual(details.ingestion.ws_token, "flat-token")

    def test_reconnect_envelope_uses_fallback_session_id(self):
        original = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "avatarParticipantIdentity": "worker-1",
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "token-old",
                },
            }
        )
        refreshed = SessionDetails.from_api(
            {
                "room": self.room.to_payload(),
                "workerStatus": "ASSIGNED",
                "inputProvider": "cartesia",
                "ingestion": {
                    "ready": True,
                    "authority": "websocket",
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "token-fresh",
                },
            },
            fallback=original,
        )
        self.assertEqual(refreshed.session_id, "session-123")
        self.assertEqual(refreshed.ingestion.ws_token, "token-fresh")
        self.assertEqual(refreshed.input_provider, "cartesia")
        self.assertEqual(refreshed.avatar_participant_identity, "worker-1")


class ServiceContractTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.room = LiveKitRoom(
            url="wss://livekit.example.test",
            token="worker-room-token",
            name="room-123",
        )

    def test_ingestion_and_open_budgets_are_240_seconds(self):
        self.assertEqual(service_module._INGESTION_READY_TIMEOUT_SECONDS, 240.0)
        self.assertEqual(service_module._WS_OPEN_TIMEOUT_SECONDS, 240.0)

    def test_safe_error_redacts_subprotocol_tokens_and_terminates(self):
        text = service_module._safe_error(
            Exception("worker saw facemode.secret-token-123 and aivatar.old-456")
        )
        self.assertNotIn("secret-token-123", text)
        self.assertNotIn("old-456", text)
        self.assertIn("facemode.<redacted>", text)

    def test_input_provider_validation(self):
        with self.assertRaises(FaceModeConfigurationError):
            FaceModeVideoService(
                api_key="api-key", room=self.room, input_provider="not-a-provider"
            )
        with self.assertRaises(FaceModeConfigurationError):
            FaceModeVideoService(
                api_key="api-key", room=self.room, input_provider=""
            )
        service = FaceModeVideoService(
            api_key="api-key", room=self.room, input_provider="Deepgram"
        )
        self.assertEqual(service.input_provider, "deepgram")

    async def test_create_session_sends_input_provider_without_wait_field(self):
        client = _RecordingClient(posts=[_Response(_ready_payload(), status=201)])
        service = FaceModeVideoService(
            api_key="api-key",
            room=self.room,
            http_session=client,
            input_provider="sarvam",
        )

        session = await service._create_session("room-123")

        self.assertEqual(len(client.post_calls), 1)
        call = client.post_calls[0]
        self.assertTrue(call["url"].endswith("/sessions"))
        body = call["json"]
        self.assertNotIn("waitForIngestion", body)
        self.assertEqual(body["inputProvider"], "sarvam")
        self.assertEqual(body["room"]["token"], "worker-room-token")
        self.assertTrue(session.ingestion.ready)

    async def test_immediate_ready_response_skips_polling(self):
        client = _RecordingClient()
        service = FaceModeVideoService(
            api_key="api-key", room=self.room, http_session=client
        )
        ready = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "ingestion": {
                    "ready": True,
                    "url": "wss://worker.example.test/ws/session-123",
                    "wsToken": "one-time-token",
                },
            }
        )

        session = await service._wait_for_ingestion(ready)

        self.assertIs(session, ready)
        self.assertEqual(client.get_calls, [])

    async def test_ingestion_poll_uses_240_second_deadline(self):
        pending = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "ingestion": {"ready": False},
            }
        )
        client = _RecordingClient(gets=[_Response(_ready_payload("polled-token"))])
        service = FaceModeVideoService(
            api_key="api-key", room=self.room, http_session=client
        )
        clock = {"now": 1000.0}

        class _Loop:
            def time(self):
                return clock["now"]

        with patch(
            "pipecat_facemode.service.asyncio.get_running_loop",
            return_value=_Loop(),
        ), patch(
            "pipecat_facemode.service.asyncio.sleep", new=AsyncMock()
        ) as sleep:
            def advance(*_args, **_kwargs):
                clock["now"] += 120.0

            sleep.side_effect = advance
            session = await service._wait_for_ingestion(pending)

        self.assertTrue(session.ingestion.ready)
        self.assertEqual(session.ingestion.ws_token, "polled-token")
        self.assertEqual(len(client.get_calls), 1)

    async def test_ingestion_poll_times_out_after_budget(self):
        pending = SessionDetails.from_api(
            {
                "sessionId": "session-123",
                "room": self.room.to_payload(),
                "ingestion": {"ready": False},
            }
        )
        client = _RecordingClient(
            gets=[
                _Response(
                    '{"sessionId":"session-123","ingestion":{"ready":false}}'
                )
            ]
            * 8
        )
        service = FaceModeVideoService(
            api_key="api-key", room=self.room, http_session=client
        )
        clock = {"now": 5000.0}

        class _Loop:
            def time(self):
                return clock["now"]

        with patch(
            "pipecat_facemode.service.asyncio.get_running_loop",
            return_value=_Loop(),
        ), patch(
            "pipecat_facemode.service.asyncio.sleep", new=AsyncMock()
        ) as sleep:
            def advance(*_args, **_kwargs):
                clock["now"] += 60.0

            sleep.side_effect = advance
            with self.assertRaisesRegex(
                FaceModeAPIError, "Timed out waiting for FaceMode ingestion"
            ):
                await service._wait_for_ingestion(pending)

        # 60s of injected time is below the 240s budget: every poll ran.
        self.assertGreaterEqual(len(client.get_calls), 4)


if __name__ == "__main__":
    unittest.main()
