"""Pipecat FrameProcessor for FaceMode LiveKit avatar sessions."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
from collections.abc import Mapping
from typing import Any

import aiohttp
import numpy as np
import websockets
from livekit import rtc
from pipecat.frames.frames import (
    CancelFrame,
    EndFrame,
    Frame,
    ImageRawFrame,
    OutputAudioRawFrame,
    StartFrame,
    TTSAudioRawFrame,
)
from pipecat.frames.frames import InterruptionFrame

try:
    from pipecat.frames.frames import StartInterruptionFrame
except ImportError:  # Pipecat 1.7 uses InterruptionFrame for this event
    StartInterruptionFrame = None  # type: ignore[assignment,misc]

try:
    from pipecat.frames.frames import StopInterruptionFrame
except ImportError:  # Pipecat 1.7 has no separate stop event
    StopInterruptionFrame = None  # type: ignore[assignment,misc]
from pipecat.processors.frame_processor import FrameDirection, FrameProcessor

from .exceptions import (
    FaceModeAPIError,
    FaceModeConfigurationError,
    FaceModeError,
    FaceModeNotReadyError,
    FaceModeProtocolError,
)
from .models import LiveKitRoom, SessionDetails, SessionRequest

try:
    from pipecat.frames.frames import OutputImageRawFrame
except ImportError:  # pragma: no cover - depends on the installed Pipecat release
    OutputImageRawFrame = None  # type: ignore[assignment,misc]

try:
    from pipecat.frames.frames import OutputVideoRawFrame
except ImportError:  # pragma: no cover - kept for older Pipecat releases
    OutputVideoRawFrame = None  # type: ignore[assignment,misc]


logger = logging.getLogger("pipecat_facemode")

_MIN_INPUT_SAMPLE_RATE = 8_000
_MAX_INPUT_SAMPLE_RATE = 48_000
_INGESTION_READY_TIMEOUT_SECONDS = 60.0
_INGESTION_INITIAL_RETRY_SECONDS = 0.25
_INGESTION_MAX_RETRY_SECONDS = 2.0
_END_ACK_TIMEOUT_SECONDS = 3.0
_PREFERRED_SAMPLE_RATES = frozenset(
    {8_000, 11_025, 12_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000}
)
_SUPPORTED_CHANNELS = frozenset({1, 2})
_WS_CLOSE_TIMEOUT = 2.0


class FaceModeVideoService(FrameProcessor):
    """Bridge Pipecat TTS audio to a FaceMode avatar and LiveKit output.

    The processor creates a FaceMode session when the pipeline receives its
    ``StartFrame``. TTS PCM is sent through FaceMode's canonical WebSocket, while
    the avatar's LiveKit audio and video tracks are converted back into Pipecat
    output frames. TTS frames are consumed by default so a pipeline does not send
    the source TTS audio and the avatar audio twice.

    ``room.token`` is the server-side worker token sent to FaceMode. A separate
    ``livekit_subscriber_token`` can be supplied for this processor's LiveKit
    connection. If it is omitted, the worker room token is also used for
    subscribing. Supplying separate tokens is recommended because LiveKit
    participant identities should not be reused by the avatar worker and the
    Pipecat process.
    """

    def __init__(
        self,
        *,
        api_key: str,
        room: LiveKitRoom | Mapping[str, Any],
        avatar_id: str = "",
        api_url: str = "https://api.facemode.io/api",
        room_name: str | None = None,
        livekit_subscriber_token: str | None = None,
        avatar_participant_identity: str | None = None,
        forward_tts_audio: bool = False,
        utterance_idle_timeout: float = 0.35,
        protocol_timeout: float = 15.0,
        api_timeout: float = 30.0,
        http_session: aiohttp.ClientSession | None = None,
        name: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Create a FaceMode Pipecat processor.

        Args:
            api_key: FaceMode REST API key.
            room: Customer room object with ``type``, ``url``, and server-side
                ``token`` fields. The token is sent to FaceMode for the worker.
            avatar_id: Optional FaceMode avatar identifier.
            api_url: FaceMode API root. Defaults to the hosted API.
            room_name: LiveKit room name. If omitted, it is read from the connected room.
            livekit_subscriber_token: Optional token for this process to subscribe.
            avatar_participant_identity: Optional identity filter for avatar tracks;
                defaults to ``facemode-avatar`` when omitted.
            forward_tts_audio: Also pass source TTS frames downstream when true.
            utterance_idle_timeout: Seconds of silence after which an utterance ends.
            protocol_timeout: WebSocket start acknowledgement timeout.
            api_timeout: REST request timeout in seconds.
            http_session: Optional caller-owned aiohttp session.
            name: Optional Pipecat processor name.
            **kwargs: Additional arguments accepted by ``FrameProcessor``.
        """
        super().__init__(name=name or "FaceModeVideoService", **kwargs)

        if not api_key:
            raise FaceModeConfigurationError("api_key is required")
        try:
            room_config = LiveKitRoom.from_value(room)
        except ValueError as error:
            raise FaceModeConfigurationError(str(error)) from error
        if not api_url:
            raise FaceModeConfigurationError("api_url is required")
        if utterance_idle_timeout <= 0:
            raise FaceModeConfigurationError("utterance_idle_timeout must be positive")
        if protocol_timeout <= 0:
            raise FaceModeConfigurationError("protocol_timeout must be positive")
        if api_timeout <= 0:
            raise FaceModeConfigurationError("api_timeout must be positive")

        self.api_key = api_key
        self.avatar_id = avatar_id
        self.api_url = api_url.rstrip("/")
        self.room_config = room_config
        self.livekit_subscriber_token = livekit_subscriber_token or room_config.token
        self.room_name = room_name
        self.avatar_participant_identity = avatar_participant_identity
        self.forward_tts_audio = forward_tts_audio
        self.utterance_idle_timeout = utterance_idle_timeout
        self.protocol_timeout = protocol_timeout
        self.api_timeout = api_timeout
        self._http_session = http_session

        self.session: SessionDetails | None = None
        self.room: rtc.Room | None = None
        self.websocket: Any = None

        self._lifecycle_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._protocol_started_event = asyncio.Event()
        self._session_ended_event = asyncio.Event()
        self._avatar_ready_event = asyncio.Event()
        self._protocol_started = False
        self._protocol_start_sent = False
        self._protocol_error: FaceModeProtocolError | None = None
        self._sequence = 0
        self._audio_sample_rate: int | None = None
        self._audio_channels: int | None = None
        self._session_log_marker: str | None = None
        self._utterance_active = False
        self._utterance_context_id: str | None = None
        self._utterance_generation = 0
        self._interrupted = False
        self._started = False
        self._stopping = False
        self._accept_tracks = False

        self._receiver_task: asyncio.Task[Any] | None = None
        self._keepalive_task: asyncio.Task[Any] | None = None
        self._utterance_end_task: asyncio.Task[Any] | None = None
        self._track_tasks: dict[str, asyncio.Task[Any]] = {}
        self._track_streams: dict[str, Any] = {}
        self._track_kinds: dict[str, Any] = {}

    @property
    def session_id(self) -> str | None:
        """Return the active FaceMode session ID, if a session exists."""
        return self.session.session_id if self.session else None

    @property
    def is_started(self) -> bool:
        """Return whether the FaceMode and LiveKit resources are active."""
        return self._started and not self._stopping

    @property
    def avatar_ready(self) -> bool:
        """Return whether FaceMode or LiveKit has reported avatar media readiness."""
        return self._avatar_ready_event.is_set()

    async def wait_for_avatar(self, timeout: float | None = 30.0) -> None:
        """Wait until FaceMode reports ``audio_ready`` or a video frame arrives."""
        try:
            if timeout is None:
                await self._avatar_ready_event.wait()
            else:
                await asyncio.wait_for(
                    self._avatar_ready_event.wait(), timeout=timeout
                )
        except asyncio.TimeoutError as error:
            raise FaceModeNotReadyError(
                "Timed out waiting for FaceMode avatar media"
            ) from error

    async def process_frame(self, frame: Frame, direction: FrameDirection) -> None:
        """Handle lifecycle, TTS, and interruption frames.

        The base implementation is deliberately called first, as required by the
        Pipecat custom ``FrameProcessor`` contract. Frames that this integration does
        not consume are always pushed in their original direction.
        """
        await super().process_frame(frame, direction)

        if isinstance(frame, StartFrame):
            await self._start_session(frame)
            try:
                await self.push_frame(frame, direction)
                self._accept_tracks = True
                self._subscribe_to_existing_tracks()
            except BaseException:
                await self._shutdown(send_end_session=False)
                raise
            return

        if isinstance(frame, EndFrame):
            await self._shutdown(send_end_session=True)
            await self.push_frame(frame, direction)
            return

        if isinstance(frame, CancelFrame):
            await self._shutdown(send_end_session=False)
            await self.push_frame(frame, direction)
            return

        stop_interruption_types = _frame_types(StopInterruptionFrame)
        if stop_interruption_types and isinstance(frame, stop_interruption_types):
            self._interrupted = False
            await self.push_frame(frame, direction)
            return

        start_interruption_types = _frame_types(StartInterruptionFrame, InterruptionFrame)
        if isinstance(frame, start_interruption_types):
            self._interrupted = True
            with contextlib.suppress(FaceModeError):
                await self._cancel_utterance()
            await self.push_frame(frame, direction)
            return

        if isinstance(frame, TTSAudioRawFrame):
            if self._started and not self._interrupted:
                await self._send_tts_frame(frame)
            if self.forward_tts_audio:
                await self.push_frame(frame, direction)
            return

        # Pipecat 1.7 emits TTSStoppedFrame after an utterance. Use a class-name
        # check so this package remains importable with versions that do not expose
        # that frame, while the required frames above stay explicit and typed.
        if frame.__class__.__name__ in {"TTSStoppedFrame", "BotStoppedSpeakingFrame"}:
            await self._end_utterance()
            await self.push_frame(frame, direction)
            return

        await self.push_frame(frame, direction)

    async def cleanup(self) -> None:
        """Release WebSocket, LiveKit, and receive-task resources."""
        await self._shutdown(send_end_session=True)
        await super().cleanup()

    async def stop(self) -> None:
        """Stop an active FaceMode session outside normal frame processing."""
        await self._shutdown(send_end_session=True)

    async def aclose(self) -> None:
        """Async-close alias for applications that manage the processor directly."""
        await self.stop()

    async def _start_session(self, frame: StartFrame) -> None:
        async with self._lifecycle_lock:
            if self._started:
                return

            self._stopping = False
            self._interrupted = False
            self._protocol_started = False
            self._protocol_start_sent = False
            self._protocol_error = None
            self._protocol_started_event = asyncio.Event()
            self._session_ended_event = asyncio.Event()
            self._avatar_ready_event = asyncio.Event()
            self._sequence = 0
            self._audio_sample_rate = _optional_int(
                getattr(frame, "audio_out_sample_rate", None)
            )
            self._audio_channels = None

            try:
                await self._connect_livekit()
                current_room_name = (
                    self.room_name
                    or self.room_config.name
                    or (self.room.name if self.room else "")
                )
                if not current_room_name:
                    raise FaceModeConfigurationError(
                        "room_name is required when LiveKit does not provide a room name"
                    )
                self.session = await self._create_session(current_room_name)
                self.session = await self._wait_for_ingestion(self.session)
                self._session_log_marker = _sanitize_session_marker(self.session.session_id)
                await self._connect_websocket()
                self._receiver_task = asyncio.create_task(
                    self._receive_events(), name="facemode-ws-receiver"
                )
                self._keepalive_task = asyncio.create_task(
                    self._keepalive_loop(), name="facemode-ws-keepalive"
                )
                await self._ensure_protocol_started(
                    self._audio_sample_rate or 24_000, 1
                )
                self._started = True
                logger.info("FaceMode Pipecat session started session=%s", self._session_log_marker)
            except BaseException:
                await self._shutdown(send_end_session=False)
                raise

    async def _connect_livekit(self) -> None:
        room = rtc.Room()
        self.room = room
        room.on("track_subscribed", self._on_track_subscribed)
        room.on("track_unsubscribed", self._on_track_unsubscribed)
        try:
            # LiveKit defaults to auto-subscribe. Pass the option explicitly when
            # supported, and keep a two-argument fallback for older SDKs.
            room_options_type = getattr(rtc, "RoomOptions", None)
            if room_options_type is not None:
                try:
                    options = room_options_type(auto_subscribe=True)
                    await room.connect(
                        self.room_config.url,
                        self.livekit_subscriber_token,
                        options=options,
                    )
                except TypeError:
                    await room.connect(self.room_config.url, self.livekit_subscriber_token)
            else:
                await room.connect(self.room_config.url, self.livekit_subscriber_token)
        except Exception as error:
            raise FaceModeNotReadyError(
                f"Unable to connect to the LiveKit room: {_safe_error(error, self.livekit_subscriber_token)}"
            ) from error

    def _subscribe_to_existing_tracks(self) -> None:
        room = self.room
        if room is None:
            return
        for participant in room.remote_participants.values():
            for publication in participant.track_publications.values():
                track = getattr(publication, "track", None)
                if track is not None:
                    self._on_track_subscribed(track, publication, participant)

    async def _create_session(self, room_name: str) -> SessionDetails:
        request = SessionRequest(
            avatar_id=self.avatar_id,
            room=self.room_config,
            room_name=room_name,
        )
        payload = request.to_payload()
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        async def post(client: aiohttp.ClientSession) -> SessionDetails:
            try:
                async with client.post(
                    f"{self.api_url}/sessions", json=payload, headers=headers
                ) as response:
                    body = await response.text()
                    if response.status >= 400:
                        raise FaceModeAPIError(
                            f"FaceMode session creation failed (HTTP {response.status})"
                        )
            except FaceModeAPIError:
                raise
            except Exception as error:
                raise FaceModeAPIError(
                    f"FaceMode session request failed: {_safe_error(error, self.api_key, self.room_config.token)}"
                ) from error

            try:
                response_payload = json.loads(body)
            except json.JSONDecodeError as error:
                raise FaceModeAPIError("FaceMode session response was not JSON") from error
            if not isinstance(response_payload, Mapping):
                raise FaceModeAPIError("FaceMode session response was not an object")
            try:
                return SessionDetails.from_api(response_payload)
            except ValueError as error:
                raise FaceModeAPIError("FaceMode session response is incomplete") from error

        timeout = aiohttp.ClientTimeout(total=self.api_timeout)
        if self._http_session is not None:
            return await post(self._http_session)
        async with aiohttp.ClientSession(timeout=timeout) as client:
            return await post(client)

    async def _wait_for_ingestion(self, initial_session: SessionDetails) -> SessionDetails:
        """Poll the session resource until the canonical WebSocket is assigned."""
        session = initial_session
        deadline = asyncio.get_running_loop().time() + _INGESTION_READY_TIMEOUT_SECONDS
        delay = _INGESTION_INITIAL_RETRY_SECONDS
        headers = {"Authorization": f"Bearer {self.api_key}"}

        async def get_status(client: aiohttp.ClientSession) -> SessionDetails:
            try:
                async with client.get(
                    f"{self.api_url}/sessions/{session.session_id}", headers=headers
                ) as response:
                    body = await response.text()
                    if response.status >= 400:
                        raise FaceModeAPIError(
                            f"FaceMode ingestion status failed (HTTP {response.status})"
                        )
            except FaceModeAPIError:
                raise
            except Exception as error:
                raise FaceModeAPIError(
                    "FaceMode ingestion status request failed: "
                    f"{_safe_error(error, self.api_key)}"
                ) from error
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as error:
                raise FaceModeAPIError("FaceMode ingestion status response was not JSON") from error
            if not isinstance(payload, Mapping):
                raise FaceModeAPIError("FaceMode ingestion status response was not an object")
            try:
                return SessionDetails.from_api(payload, fallback=session)
            except ValueError as error:
                raise FaceModeAPIError("FaceMode ingestion status response is incomplete") from error

        timeout = aiohttp.ClientTimeout(total=self.api_timeout)
        owned_session: aiohttp.ClientSession | None = None
        client = self._http_session
        if client is None:
            owned_session = aiohttp.ClientSession(timeout=timeout)
            client = owned_session
        try:
            while not session.ingestion.ready:
                if asyncio.get_running_loop().time() >= deadline:
                    raise FaceModeAPIError("Timed out waiting for FaceMode ingestion assignment")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, _INGESTION_MAX_RETRY_SECONDS)
                session = await get_status(client)
                if session.worker_status in {"FAILED", "ENDED"}:
                    raise FaceModeAPIError(
                        f"FaceMode ingestion worker entered {session.worker_status.lower()} state"
                    )
        finally:
            if owned_session is not None:
                await owned_session.close()

        if not session.ingestion.url or not session.ingestion.ws_token:
            raise FaceModeAPIError(
                "FaceMode ingestion assignment is missing WebSocket credentials"
            )
        return session

    async def _connect_websocket(self) -> None:
        if self.session is None:
            raise FaceModeProtocolError("FaceMode session has not been created")
        subprotocols = [f"aivatar.{self.session.ingestion.ws_token}"]
        try:
            # pyproject.toml requires websockets>=14, where additional_headers is
            # the supported custom-handshake API. Do not retry without the headers:
            # they may contain the backend-issued strict worker affinity directive.
            self.websocket = await websockets.connect(
                self.session.ingestion.url,
                subprotocols=subprotocols,
                additional_headers=dict(self.session.ingestion.headers),
                max_size=2**20,
                ping_interval=None,
                compression=None,
                open_timeout=60,
            )
        except Exception as error:
            raise FaceModeProtocolError(
                f"Unable to connect to FaceMode WebSocket: {_safe_error(error, self.session.ingestion.ws_token)}"
            ) from error

    async def _send_tts_frame(self, frame: TTSAudioRawFrame) -> None:
        sample_rate = _optional_int(getattr(frame, "sample_rate", None))
        channels = _optional_int(getattr(frame, "num_channels", None))
        if sample_rate is None or channels is None:
            raise FaceModeProtocolError("TTS audio is missing sample rate or channel count")
        await self._ensure_protocol_started(sample_rate, channels)

        context_id = getattr(frame, "context_id", None)
        context_id = str(context_id) if context_id is not None else None
        if self._utterance_active and context_id and self._utterance_context_id:
            if context_id != self._utterance_context_id:
                await self._end_utterance()
        if not self._utterance_active:
            await self._send_control("start_utterance")
            self._utterance_active = True
            self._utterance_context_id = context_id

        audio = bytes(getattr(frame, "audio", b""))
        if not audio:
            return
        if len(audio) % (2 * channels):
            raise FaceModeProtocolError("TTS PCM payload is not channel aligned")
        await self._send_binary(audio)
        self._arm_utterance_end_timer()

    async def _ensure_protocol_started(self, sample_rate: int, channels: int) -> None:
        if self._protocol_error is not None:
            raise self._protocol_error
        if isinstance(sample_rate, bool) or not isinstance(sample_rate, int):
            raise FaceModeProtocolError("TTS sample rate for FaceMode must be an integer")
        if not _MIN_INPUT_SAMPLE_RATE <= sample_rate <= _MAX_INPUT_SAMPLE_RATE:
            raise FaceModeProtocolError(
                f"TTS sample rate for FaceMode must be between {_MIN_INPUT_SAMPLE_RATE} "
                f"and {_MAX_INPUT_SAMPLE_RATE}: {sample_rate}"
            )
        if sample_rate not in _PREFERRED_SAMPLE_RATES:
            logger.info("Using uncommon Pipecat TTS sample rate: %s", sample_rate)
        if channels not in _SUPPORTED_CHANNELS:
            raise FaceModeProtocolError(
                f"Unsupported TTS channel count for FaceMode: {channels}"
            )
        if self.websocket is None or self.session is None:
            raise FaceModeProtocolError("FaceMode WebSocket is not connected")

        if self._protocol_started or self._protocol_start_sent:
            if self._audio_sample_rate != sample_rate or self._audio_channels != channels:
                raise FaceModeProtocolError(
                    "TTS audio format changed during the FaceMode session"
                )
            if self._protocol_started:
                return

        async with self._send_lock:
            if self._protocol_started:
                return
            if self._protocol_start_sent:
                start_already_sent = True
            else:
                start_already_sent = False
                self._protocol_start_sent = True
            self._audio_sample_rate = sample_rate
            self._audio_channels = channels
            if not start_already_sent:
                await self._send_json(
                    {
                        "type": "start",
                        "session_id": self.session.session_id,
                        "audio_encoding": "pcm_s16le",
                        "sample_rate": sample_rate,
                        "channels": channels,
                        "avatar_id": self.avatar_id,
                        "metadata": {"source": "pipecat-facemode"},
                    },
                    assume_lock=True,
                )

        try:
            await asyncio.wait_for(
                self._protocol_started_event.wait(), timeout=self.protocol_timeout
            )
        except asyncio.TimeoutError as error:
            raise FaceModeProtocolError(
                "FaceMode did not acknowledge protocol negotiation"
            ) from error
        if self._protocol_error is not None:
            raise self._protocol_error
        if not self._protocol_started:
            raise FaceModeProtocolError("FaceMode protocol negotiation did not start")

    async def _send_control(self, message_type: str) -> None:
        if message_type == "ping":
            await self._send_json({"type": "ping", "ts": int(time.time() * 1000)})
            return
        await self._send_json({"type": message_type, "seq": self._next_sequence()})

    async def _send_binary(self, audio: bytes) -> None:
        if self._protocol_error is not None:
            raise self._protocol_error
        async with self._send_lock:
            websocket = self.websocket
            if websocket is None:
                raise FaceModeProtocolError("FaceMode WebSocket is not connected")
            try:
                await websocket.send(audio)
            except Exception as error:
                raise FaceModeProtocolError(
                    f"Unable to send audio to FaceMode: {_safe_error(error, self.session.ingestion.ws_token if self.session else None)}"
                ) from error

    async def _send_json(self, message: dict[str, Any], *, assume_lock: bool = False) -> None:
        if assume_lock:
            await self._send_json_locked(message)
            return
        async with self._send_lock:
            await self._send_json_locked(message)

    async def _send_json_locked(self, message: dict[str, Any]) -> None:
        websocket = self.websocket
        if websocket is None:
            return
        try:
            await websocket.send(json.dumps(message, separators=(",", ":")))
        except Exception as error:
            protocol_error = FaceModeProtocolError(
                f"Unable to send FaceMode protocol message: {_safe_error(error)}"
            )
            self._set_protocol_error(protocol_error)
            raise protocol_error from error

    async def _receive_events(self) -> None:
        websocket = self.websocket
        if websocket is None:
            return
        try:
            async for raw in websocket:
                if isinstance(raw, (bytes, bytearray, memoryview)):
                    continue
                try:
                    message = json.loads(raw)
                except (TypeError, json.JSONDecodeError) as error:
                    raise FaceModeProtocolError(
                        "FaceMode WebSocket message was not JSON"
                    ) from error
                if not isinstance(message, Mapping):
                    raise FaceModeProtocolError(
                        "FaceMode WebSocket message was not an object"
                    )
                message_type = str(message.get("type", ""))
                if message_type == "started":
                    if self.session is None:
                        raise FaceModeProtocolError("FaceMode session is not initialized")
                    if str(message.get("session_id", "")) != self.session.session_id:
                        raise FaceModeProtocolError(
                            "FaceMode started response session ID did not match"
                        )
                    if (
                        message.get("server_sample_rate") != 48_000
                        or message.get("server_channels") != 1
                    ):
                        raise FaceModeProtocolError(
                            "FaceMode server reported an unsupported canonical audio format"
                        )
                    self._protocol_started = True
                    self._protocol_started_event.set()
                    logger.info("FaceMode protocol started session=%s", self._session_log_marker)
                elif message_type == "audio_ready":
                    self._avatar_ready_event.set()
                elif message_type == "utterance_ended":
                    logger.debug(
                        "FaceMode utterance ended seq=%s cycles=%s",
                        message.get("seq"),
                        message.get("cycles"),
                    )
                elif message_type == "pong":
                    logger.debug("FaceMode keepalive acknowledged")
                elif message_type == "session_ending":
                    raise FaceModeProtocolError(
                        f"FaceMode session is ending: {_safe_text(message.get('reason'), 'unknown')}"
                    )
                elif message_type == "ended":
                    self._session_ended_event.set()
                    break
                elif message_type == "error":
                    code = _safe_text(message.get("code"), "INTERNAL_ERROR")
                    detail = _safe_text(message.get("message"), "FaceMode protocol error")
                    protocol_error = FaceModeProtocolError(f"{code}: {detail}")
                    self._set_protocol_error(protocol_error)
                    logger.error("FaceMode protocol error code=%s", code)
                    if bool(message.get("fatal", False)) or not self._protocol_started:
                        break
                else:
                    logger.debug("FaceMode WebSocket unhandled message type: %s", message_type)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not self._stopping:
                protocol_error = (
                    error
                    if isinstance(error, FaceModeProtocolError)
                    else FaceModeProtocolError(
                        "FaceMode WebSocket receiver stopped: "
                        f"{_safe_error(error)}{_websocket_close_details(error)}"
                    )
                )
                self._set_protocol_error(protocol_error)
                logger.warning("%s", protocol_error)
        else:
            if not self._stopping:
                self._set_protocol_error(
                    FaceModeProtocolError(
                        "FaceMode WebSocket closed unexpectedly"
                        f"{_websocket_close_details(websocket)}"
                    )
                )

    async def _keepalive_loop(self) -> None:
        try:
            await self._protocol_started_event.wait()
            if not self._protocol_started or self._protocol_error is not None:
                return
            while not self._stopping:
                await asyncio.sleep(15.0)
                if self.websocket is None or self._stopping:
                    return
                try:
                    await self._send_control("ping")
                except FaceModeError as error:
                    if not self._stopping:
                        self._set_protocol_error(error)
                        logger.warning("FaceMode keepalive stopped: %s", _safe_error(error))
                    return
        except asyncio.CancelledError:
            raise

    async def _end_utterance(self) -> None:
        self._cancel_utterance_timer()
        if not self._utterance_active:
            return
        self._utterance_active = False
        self._utterance_context_id = None
        if self._protocol_started and self.websocket is not None and not self._stopping:
            await self._send_control("end_utterance")

    async def _cancel_utterance(self) -> None:
        self._cancel_utterance_timer()
        was_active = self._utterance_active
        self._utterance_active = False
        self._utterance_context_id = None
        if was_active and self._protocol_started and self.websocket is not None:
            await self._send_control("cancel_utterance")

    def _arm_utterance_end_timer(self) -> None:
        self._cancel_utterance_timer()
        self._utterance_generation += 1
        generation = self._utterance_generation
        self._utterance_end_task = asyncio.create_task(
            self._end_utterance_after_idle(generation), name="facemode-utterance-end"
        )

    async def _end_utterance_after_idle(self, generation: int) -> None:
        try:
            await asyncio.sleep(self.utterance_idle_timeout)
            if generation == self._utterance_generation and not self._stopping:
                self._utterance_end_task = None
                await self._end_utterance()
        except asyncio.CancelledError:
            raise
        except FaceModeError as error:
            if not self._stopping:
                logger.warning("Unable to end FaceMode utterance: %s", _safe_error(error))

    def _cancel_utterance_timer(self) -> None:
        task = self._utterance_end_task
        self._utterance_end_task = None
        if (
            task is not None
            and task is not asyncio.current_task()
            and not task.done()
        ):
            task.cancel()

    def _on_track_subscribed(
        self,
        track: Any,
        publication: Any,
        participant: Any,
    ) -> None:
        if (
            self._stopping
            or not self._accept_tracks
            or not self._is_avatar_participant(participant)
        ):
            return
        kind = getattr(track, "kind", None)
        if not _is_audio_track_kind(kind) and not _is_video_track_kind(kind):
            return
        key = str(
            getattr(publication, "sid", None)
            or getattr(track, "sid", None)
            or id(track)
        )
        self._cancel_track_task(key)
        self._track_kinds[key] = kind
        if _is_video_track_kind(kind):
            task = asyncio.create_task(
                self._consume_video_track(key, track), name="facemode-livekit-video"
            )
        else:
            task = asyncio.create_task(
                self._consume_audio_track(key, track), name="facemode-livekit-audio"
            )
        self._track_tasks[key] = task

    def _on_track_unsubscribed(self, track: Any, publication: Any, participant: Any) -> None:
        del track, participant
        key = str(getattr(publication, "sid", None) or "")
        if key:
            self._cancel_track_task(key)

    def _is_avatar_participant(self, participant: Any) -> bool:
        identity = str(getattr(participant, "identity", ""))
        local_identity = ""
        if self.room is not None:
            local_participant = getattr(self.room, "local_participant", None)
            local_identity = str(getattr(local_participant, "identity", ""))
        if identity and identity == local_identity:
            return False
        expected = self.avatar_participant_identity or (
            self.session.avatar_participant_identity if self.session else None
        ) or "facemode-avatar"
        return identity == expected

    async def _consume_audio_track(self, key: str, track: Any) -> None:
        stream = None
        try:
            try:
                stream = rtc.AudioStream(track, capacity=8)
            except TypeError:
                stream = rtc.AudioStream(track)
            self._track_streams[key] = stream
            async for event in stream:
                if self._stopping:
                    break
                audio_frame = getattr(event, "frame", event)
                audio = bytes(getattr(audio_frame, "data", b""))
                if not audio:
                    continue
                output = OutputAudioRawFrame(
                    audio=audio,
                    sample_rate=_optional_int(getattr(audio_frame, "sample_rate", None)) or 48_000,
                    num_channels=_optional_int(getattr(audio_frame, "num_channels", None)) or 1,
                )
                await self.push_frame(output, FrameDirection.DOWNSTREAM)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not self._stopping:
                logger.warning(
                    "FaceMode LiveKit audio receiver stopped: %s", _safe_error(error)
                )
        finally:
            await _close_async_resource(stream)
            if self._track_streams.get(key) is stream:
                self._track_streams.pop(key, None)

    async def _consume_video_track(self, key: str, track: Any) -> None:
        stream = None
        try:
            stream = _make_video_stream(track)
            self._track_streams[key] = stream
            async for event in stream:
                if self._stopping:
                    break
                video_frame = getattr(event, "frame", event)
                output = _video_frame_to_pipecat(video_frame)
                if output is not None:
                    self._avatar_ready_event.set()
                    await self.push_frame(output, FrameDirection.DOWNSTREAM)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not self._stopping:
                logger.warning(
                    "FaceMode LiveKit video receiver stopped: %s", _safe_error(error)
                )
        finally:
            await _close_async_resource(stream)
            if self._track_streams.get(key) is stream:
                self._track_streams.pop(key, None)

    def _cancel_track_task(self, key: str) -> None:
        task = self._track_tasks.pop(key, None)
        self._track_kinds.pop(key, None)
        if task is not None and not task.done():
            task.cancel()

    async def _shutdown(self, *, send_end_session: bool) -> None:
        async with self._lifecycle_lock:
            if self._stopping and not self._started and self.websocket is None and self.room is None:
                return
            self._stopping = True
            self._accept_tracks = False
            self._cancel_utterance_timer()

            websocket = self.websocket
            if send_end_session and websocket is not None and self._protocol_started:
                with contextlib.suppress(FaceModeError):
                    if self._utterance_active:
                        await self._send_control("end_utterance")
                    await self._send_control("end_session")
                    await asyncio.wait_for(
                        self._session_ended_event.wait(),
                        timeout=_END_ACK_TIMEOUT_SECONDS,
                    )
            self._utterance_active = False
            self._utterance_context_id = None

            current = asyncio.current_task()
            tasks = [
                self._keepalive_task,
                self._receiver_task,
                *self._track_tasks.values(),
            ]
            self._keepalive_task = None
            self._receiver_task = None
            self._track_tasks.clear()
            self._track_kinds.clear()
            for task in tasks:
                if task is not None and task is not current and not task.done():
                    task.cancel()
            for task in tasks:
                if task is not None and task is not current:
                    with contextlib.suppress(asyncio.CancelledError, Exception):
                        await task

            for stream in list(self._track_streams.values()):
                await _close_async_resource(stream)
            self._track_streams.clear()

            self.websocket = None
            if websocket is not None:
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(websocket.close(), timeout=_WS_CLOSE_TIMEOUT)

            room = self.room
            self.room = None
            await _disconnect_room(room)

            self.session = None
            self._session_log_marker = None
            self._protocol_started = False
            self._protocol_start_sent = False
            self._protocol_error = None
            self._protocol_started_event.set()
            self._avatar_ready_event.clear()
            self._started = False
            self._stopping = False

    def _set_protocol_error(self, error: FaceModeProtocolError) -> None:
        self._protocol_error = error
        self._protocol_started_event.set()

    def _next_sequence(self) -> int:
        sequence = self._sequence
        self._sequence += 1
        return sequence


# Short aliases are useful for applications that do not use the video-specific name.
FaceModeService = FaceModeVideoService
FaceModeProcessor = FaceModeVideoService


def _is_audio_track_kind(kind: Any) -> bool:
    track_kind = getattr(rtc, "TrackKind", None)
    return kind == getattr(track_kind, "KIND_AUDIO", object()) or str(kind).lower() in {
        "audio",
        "kind_audio",
    }


def _is_video_track_kind(kind: Any) -> bool:
    track_kind = getattr(rtc, "TrackKind", None)
    return kind == getattr(track_kind, "KIND_VIDEO", object()) or str(kind).lower() in {
        "video",
        "kind_video",
    }


def _make_video_stream(track: Any) -> Any:
    video_buffer_type = getattr(rtc, "VideoBufferType", None)
    rgb24 = getattr(video_buffer_type, "RGB24", None) if video_buffer_type else None
    try:
        if rgb24 is not None:
            return rtc.VideoStream(track, format=rgb24, capacity=1)
        return rtc.VideoStream(track, capacity=1)
    except TypeError:
        return rtc.VideoStream(track)


def _video_frame_to_pipecat(video_frame: Any) -> Frame | None:
    width = _optional_int(getattr(video_frame, "width", None)) or 0
    height = _optional_int(getattr(video_frame, "height", None)) or 0
    if width <= 0 or height <= 0:
        return None

    data = getattr(video_frame, "data", b"")
    try:
        pixels = np.frombuffer(data, dtype=np.uint8)
    except (TypeError, ValueError):
        pixels = np.asarray(data, dtype=np.uint8)
    expected_pixels = width * height
    if pixels.size >= expected_pixels * 4:
        channels = 4
        pixel_format = "RGBA"
    elif pixels.size >= expected_pixels * 3:
        channels = 3
        pixel_format = "RGB"
    else:
        return None
    image = pixels[: expected_pixels * channels].reshape((height, width, channels)).tobytes()
    return _make_output_video_frame(image, (width, height), pixel_format)


def _make_output_video_frame(
    image: bytes,
    size: tuple[int, int],
    pixel_format: str,
) -> Frame:
    constructors = []
    if OutputVideoRawFrame is not None:
        constructors.append(OutputVideoRawFrame)
    if OutputImageRawFrame is not None:
        constructors.append(OutputImageRawFrame)
    constructors.append(ImageRawFrame)

    for constructor in constructors:
        try:
            return constructor(image=image, size=size, format=pixel_format)
        except TypeError:
            with contextlib.suppress(TypeError):
                return constructor(image, size, pixel_format)
    raise FaceModeNotReadyError("Installed Pipecat version cannot construct a video output frame")


async def _disconnect_room(room: Any) -> None:
    if room is None:
        return
    disconnect = getattr(room, "disconnect", None)
    if disconnect is None:
        return
    try:
        result = disconnect()
    except Exception:
        return
    if hasattr(result, "__await__"):
        with contextlib.suppress(Exception):
            await result


async def _close_async_resource(resource: Any) -> None:
    if resource is None:
        return
    close = getattr(resource, "aclose", None)
    if close is None:
        close = getattr(resource, "close", None)
    if close is None:
        return
    try:
        result = close()
    except Exception:
        return
    if hasattr(result, "__await__"):
        with contextlib.suppress(Exception):
            await result


def _frame_types(*candidates: Any) -> tuple[type, ...]:
    return tuple(candidate for candidate in candidates if isinstance(candidate, type))


def _optional_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_text(value: Any, fallback: str) -> str:
    text = str(value) if value is not None else ""
    return text[:300] or fallback


def _sanitize_session_marker(session_id: str) -> str:
    return "".join(
        char if char.isascii() and (char.isalnum() or char in "_-") else "_"
        for char in session_id
    )[:96]


def _websocket_close_details(value: Any) -> str:
    received = getattr(value, "rcvd", None)
    code = getattr(value, "close_code", None) or getattr(value, "code", None)
    reason = getattr(value, "close_reason", None) or getattr(value, "reason", None)
    if received is not None:
        code = code or getattr(received, "code", None)
        reason = reason or getattr(received, "reason", None)
    reason_text = str(reason or "").strip()[:160]
    suffix = f", reason={reason_text}" if reason_text else ""
    return f" (code={code if code is not None else 'unknown'}{suffix})"


def _safe_error(error: BaseException, *secrets: str | None) -> str:
    """Return an exception string without common credential-shaped values."""
    text = str(error)
    for secret in secrets:
        if secret:
            text = text.replace(secret, "<redacted>")
    for marker in ("Bearer ", "aivatar."):
        while marker in text:
            start = text.index(marker) + len(marker)
            end = text.find(" ", start)
            if end < 0:
                end = len(text)
            text = f"{text[:start]}<redacted>{text[end:]}"
    return text[:300] or error.__class__.__name__
