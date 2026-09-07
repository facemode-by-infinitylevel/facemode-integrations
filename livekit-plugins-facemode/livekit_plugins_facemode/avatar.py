"""FaceMode AvatarSession for LiveKit Agents."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import logging
import time
from collections.abc import Mapping
from typing import Any

import aiohttp
import websockets
from livekit import rtc

from .exceptions import FaceModeAPIError, FaceModeNotReadyError, FaceModeProtocolError
from .models import LiveKitRoom, SessionDetails, SessionRequest

try:
    from livekit.agents.voice.avatar import AvatarSession as BaseAvatarSession
except ImportError:
    from livekit.agents.voice import AvatarSession as BaseAvatarSession

try:
    from livekit.agents.voice.io import AudioOutput, AudioOutputCapabilities
except ImportError:
    AudioOutput = None
    AudioOutputCapabilities = None

logger = logging.getLogger("livekit_plugins_facemode")

_MIN_INPUT_SAMPLE_RATE = 8_000
_MAX_INPUT_SAMPLE_RATE = 48_000
_INGESTION_READY_TIMEOUT_SECONDS = 60.0
_INGESTION_INITIAL_RETRY_SECONDS = 0.25
_INGESTION_MAX_RETRY_SECONDS = 2.0
_PREFERRED_SAMPLE_RATES = frozenset(
    {8_000, 11_025, 12_000, 16_000, 22_050, 24_000, 32_000, 44_100, 48_000}
)


class _FaceModeAudioOutput(AudioOutput):
    def __init__(self, owner: "AvatarSession"):
        super().__init__(
            label="FaceModeAudioOutput",
            next_in_chain=None,
            sample_rate=None,
            capabilities=AudioOutputCapabilities(pause=False),
        )
        self.owner = owner
        self._utterance_started = False

    async def capture_frame(self, frame: rtc.AudioFrame) -> None:
        await self.owner._drain_control_tasks()
        await super().capture_frame(frame)
        if not self._utterance_started:
            await self.owner._ensure_protocol_started(frame.sample_rate, frame.num_channels)
            await self.owner._send_start_utterance()
            self._utterance_started = True
        await self.owner._send_tts_audio(frame)

    def flush(self) -> None:
        super().flush()
        if self._utterance_started:
            self.owner._schedule_control(self._finish_utterance(), "facemode-end-utterance")
            self._utterance_started = False

    def clear_buffer(self) -> None:
        if self._utterance_started:
            self.owner._schedule_control(
                self.owner._send_cancel_utterance(), "facemode-cancel-utterance"
            )
        self._utterance_started = False

    async def _finish_utterance(self) -> None:
        await self.owner._send_end_utterance()


class AvatarSession(BaseAvatarSession):
    def __init__(
        self,
        *,
        api_key: str,
        avatar_id: str = "",
        api_url: str = "https://api.facemode.io/api",
        avatar_participant_identity: str = "facemode-avatar",
        avatar_participant_name: str = "FaceMode Avatar",
    ):
        super().__init__()
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.avatar_id = avatar_id
        self.api_url = api_url.rstrip("/")
        self.avatar_participant_identity = avatar_participant_identity
        self.avatar_participant_name = avatar_participant_name
        self.session: SessionDetails | None = None
        self._room: rtc.Room | None = None
        self._agent_session = None
        self._websocket = None
        self._receiver_task: asyncio.Task | None = None
        self._keepalive_task: asyncio.Task | None = None
        self._join_task: asyncio.Task | None = None
        self._control_tasks: set[asyncio.Task] = set()
        self._protocol_started = asyncio.Event()
        self._protocol_response = asyncio.Event()
        self._avatar_ready = asyncio.Event()
        self._session_ended = asyncio.Event()
        self._protocol_error: FaceModeProtocolError | None = None
        self._protocol_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._audio_sample_rate: int | None = None
        self._audio_channels: int | None = None
        self._session_log_marker: str | None = None
        self._sequence = 0
        self._started_once = False
        self._stopped = False

    @property
    def avatar_identity(self) -> str:
        return self.avatar_participant_identity

    @property
    def provider(self) -> str:
        return "facemode"

    @property
    def session_id(self) -> str | None:
        return self.session.session_id if self.session else None

    async def start(
        self,
        agent_session,
        livekit_room: rtc.Room | None = None,
        *,
        room: LiveKitRoom | Mapping[str, Any] | rtc.Room | None = None,
        room_config: LiveKitRoom | Mapping[str, Any] | None = None,
    ) -> None:
        """Start the avatar with a typed customer LiveKit room configuration.

        ``livekit_room`` is the already-connected AgentSession room used for
        media. ``room`` carries the server-side room credentials sent to
        FaceMode. ``room_config`` is an explicit alias for callers that prefer
        to avoid the two meanings of the word room.
        """
        if self._started_once:
            raise RuntimeError("AvatarSession.start() may only be called once per instance")
        if isinstance(room, rtc.Room):
            if livekit_room is not None:
                raise ValueError("livekit_room was provided more than once")
            livekit_room = room
            room = None
        if room is not None and room_config is not None:
            raise ValueError("pass either room or room_config, not both")
        room_value = room_config if room_config is not None else room
        if livekit_room is None:
            raise ValueError("livekit_room is required")
        if room_value is None:
            raise ValueError("room is required")
        try:
            livekit_config = LiveKitRoom.from_value(room_value)
        except ValueError as error:
            raise ValueError(str(error)) from error
        room_name = (
            livekit_config.name
            or str(getattr(livekit_room, "name", "") or "")
            or _token_room_hint(livekit_config.token)
        )
        if not room_name:
            raise ValueError("the exact LiveKit room name is required")

        if self.avatar_participant_identity == "facemode-avatar":
            token_identity = _token_identity_hint(livekit_config.token)
            if token_identity:
                self.avatar_participant_identity = token_identity
        self._started_once = True
        self._stopped = False
        await super().start(agent_session, livekit_room)
        self._room = livekit_room
        self._agent_session = agent_session
        try:
            self.session = await self._create_session(livekit_config, room_name)
            self.session = await self._wait_for_ingestion(self.session)
            self._session_log_marker = _sanitize_session_marker(self.session.session_id)
            if (
                self.avatar_participant_identity == "facemode-avatar"
                and self.session.avatar_participant_identity
            ):
                self.avatar_participant_identity = self.session.avatar_participant_identity
            self._websocket = await self._connect_websocket()
            self._receiver_task = asyncio.create_task(
                self._receive_events(), name="facemode-ws-events"
            )
            self._keepalive_task = asyncio.create_task(
                self._keepalive_loop(), name="facemode-ws-keepalive"
            )
            tts = getattr(agent_session, "tts", None)
            if tts is not None:
                await self._ensure_protocol_started(
                    int(tts.sample_rate), int(tts.num_channels)
                )
            if getattr(agent_session, "output", None) is None:
                raise FaceModeAPIError("LiveKit AgentSession output is not initialized")
            agent_session.output.replace_audio_tail(_FaceModeAudioOutput(self))
            self._join_task = asyncio.create_task(
                self._wait_for_video_track(), name="facemode-avatar-join"
            )
            logger.info("FaceMode avatar session started session=%s", self._session_log_marker)
        except asyncio.CancelledError:
            with contextlib.suppress(Exception):
                await self.stop()
            raise
        except Exception as error:
            with contextlib.suppress(Exception):
                await self.stop()
            if isinstance(error, (FaceModeAPIError, FaceModeProtocolError)):
                raise
            raise FaceModeAPIError(
                f"Unable to start FaceMode avatar session: {error.__class__.__name__}"
            ) from error

    async def wait_for_join(self, *, timeout: float | None = 30.0) -> None:
        if self._join_task is None:
            return
        try:
            if timeout is None:
                await self._join_task
            else:
                await asyncio.wait_for(asyncio.shield(self._join_task), timeout=timeout)
        except asyncio.TimeoutError as error:
            raise FaceModeNotReadyError("Timed out waiting for FaceMode avatar video track") from error

    async def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        await self._cancel_task(self._keepalive_task)
        self._keepalive_task = None
        with contextlib.suppress(Exception):
            await self._drain_control_tasks()

        websocket = self._websocket
        if websocket is not None:
            if self._protocol_started.is_set():
                with contextlib.suppress(Exception):
                    await self._send_control("end_session", self._next_sequence())
                    await asyncio.wait_for(self._session_ended.wait(), timeout=3.0)
            with contextlib.suppress(Exception):
                await websocket.close()

        await self._cancel_task(self._receiver_task)
        self._receiver_task = None
        await self._cancel_task(self._join_task)
        self._join_task = None
        for task in tuple(self._control_tasks):
            task.cancel()
        if self._control_tasks:
            await asyncio.gather(*tuple(self._control_tasks), return_exceptions=True)
        self._control_tasks.clear()
        self._websocket = None
        with contextlib.suppress(Exception):
            await super().aclose()

    async def aclose(self) -> None:
        await self.stop()

    async def _create_session(self, room: LiveKitRoom, room_name: str) -> SessionDetails:
        payload = SessionRequest(
            avatar_id=self.avatar_id, room=room, room_name=room_name
        ).to_payload()
        headers = {"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"}
        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as client:
                async with client.post(f"{self.api_url}/sessions", json=payload, headers=headers) as response:
                    body = await response.text()
                    if response.status >= 400:
                        raise FaceModeAPIError(
                            f"FaceMode session creation failed ({response.status})"
                        )
                    try:
                        data = json.loads(body)
                    except json.JSONDecodeError as error:
                        raise FaceModeAPIError("FaceMode session response was not JSON") from error
            if not isinstance(data, Mapping):
                raise FaceModeAPIError("FaceMode session response was not an object")
            try:
                return SessionDetails.from_api(data)
            except ValueError as error:
                raise FaceModeAPIError("FaceMode session response was incomplete") from error
        except FaceModeAPIError:
            raise
        except Exception as error:
            raise FaceModeAPIError(
                f"FaceMode session request failed: {error.__class__.__name__}"
            ) from error

    async def _wait_for_ingestion(self, initial_session: SessionDetails) -> SessionDetails:
        """Poll the public session resource until its ingestion connection is usable."""
        session = initial_session
        deadline = asyncio.get_running_loop().time() + _INGESTION_READY_TIMEOUT_SECONDS
        delay = _INGESTION_INITIAL_RETRY_SECONDS
        timeout = aiohttp.ClientTimeout(total=30)
        headers = {"Authorization": f"Bearer {self.api_key}"}

        async with aiohttp.ClientSession(timeout=timeout) as client:
            while not session.ingestion.ready:
                if asyncio.get_running_loop().time() >= deadline:
                    raise FaceModeAPIError("Timed out waiting for FaceMode ingestion assignment")
                await asyncio.sleep(delay)
                delay = min(delay * 1.5, _INGESTION_MAX_RETRY_SECONDS)
                try:
                    async with client.get(
                        f"{self.api_url}/sessions/{session.session_id}", headers=headers
                    ) as response:
                        body = await response.text()
                        if response.status >= 400:
                            raise FaceModeAPIError(
                                f"FaceMode ingestion status failed ({response.status})"
                            )
                        data = json.loads(body)
                except FaceModeAPIError:
                    raise
                except json.JSONDecodeError as error:
                    raise FaceModeAPIError(
                        "FaceMode ingestion status response was not JSON"
                    ) from error
                except Exception as error:
                    raise FaceModeAPIError(
                        f"FaceMode ingestion status request failed: {error.__class__.__name__}"
                    ) from error

                if not isinstance(data, Mapping):
                    raise FaceModeAPIError("FaceMode ingestion status response was not an object")
                try:
                    session = SessionDetails.from_api(data, fallback=session)
                except ValueError as error:
                    raise FaceModeAPIError(
                        "FaceMode ingestion status response was incomplete"
                    ) from error
                if session.worker_status in {"FAILED", "ENDED"}:
                    raise FaceModeAPIError(
                        f"FaceMode ingestion worker entered {session.worker_status.lower()} state"
                    )

        if not session.ingestion.url or not session.ingestion.ws_token:
            raise FaceModeAPIError(
                "FaceMode ingestion assignment is missing WebSocket credentials"
            )
        return session

    async def _connect_websocket(self):
        """Open the canonical ingestion socket with backend-issued headers intact."""
        if self.session is None:
            raise FaceModeProtocolError("FaceMode session is not initialized")
        ingestion = self.session.ingestion
        if not ingestion.ready or not ingestion.url or not ingestion.ws_token:
            raise FaceModeProtocolError("FaceMode ingestion assignment is not ready")
        return await websockets.connect(
            ingestion.url,
            subprotocols=[f"aivatar.{ingestion.ws_token}"],
            additional_headers=dict(ingestion.headers),
            max_size=2**20,
            ping_interval=None,
            compression=None,
            open_timeout=60,
        )

    async def _send_start_utterance(self) -> None:
        await self._send_control("start_utterance", self._next_sequence())

    async def _send_end_utterance(self) -> None:
        if self._protocol_started.is_set():
            await self._send_control("end_utterance", self._next_sequence())

    async def _send_cancel_utterance(self) -> None:
        if self._protocol_started.is_set():
            await self._send_control("cancel_utterance", self._next_sequence())

    async def _send_tts_audio(self, frame: rtc.AudioFrame) -> None:
        await self._ensure_protocol_started(frame.sample_rate, frame.num_channels)
        self._raise_protocol_error()
        if self._websocket is None:
            raise FaceModeProtocolError("FaceMode WebSocket is not connected")
        audio = frame.data.tobytes()
        alignment = 2 * frame.num_channels
        if len(audio) % alignment:
            raise FaceModeProtocolError("LiveKit TTS PCM payload is not channel aligned")
        async with self._send_lock:
            await self._websocket.send(audio)

    async def _ensure_protocol_started(self, sample_rate: int, channels: int) -> None:
        async with self._protocol_lock:
            self._raise_protocol_error()
            if self._protocol_started.is_set():
                if (
                    self._audio_sample_rate != sample_rate
                    or self._audio_channels != channels
                ):
                    raise FaceModeProtocolError(
                        "LiveKit TTS audio format changed during the FaceMode session"
                    )
                return
            if isinstance(sample_rate, bool) or not isinstance(sample_rate, int):
                raise FaceModeProtocolError("LiveKit TTS sample rate must be an integer")
            if not _MIN_INPUT_SAMPLE_RATE <= sample_rate <= _MAX_INPUT_SAMPLE_RATE:
                raise FaceModeProtocolError(
                    f"LiveKit TTS sample rate must be between {_MIN_INPUT_SAMPLE_RATE} "
                    f"and {_MAX_INPUT_SAMPLE_RATE}: {sample_rate}"
                )
            if sample_rate not in _PREFERRED_SAMPLE_RATES:
                logger.info("Using uncommon LiveKit TTS sample rate: %s", sample_rate)
            if channels not in (1, 2):
                raise FaceModeProtocolError(f"Unsupported LiveKit TTS channel count: {channels}")
            if self._websocket is None or self.session is None:
                raise FaceModeProtocolError("FaceMode WebSocket is not connected")
            self._audio_sample_rate = sample_rate
            self._audio_channels = channels
            async with self._send_lock:
                await self._websocket.send(json.dumps({
                    "type": "start",
                    "session_id": self.session.session_id,
                    "audio_encoding": "pcm_s16le",
                    "sample_rate": int(sample_rate),
                    "channels": int(channels),
                    "avatar_id": self.avatar_id,
                    "metadata": {"source": "livekit-agents"},
                }, separators=(",", ":")))
            try:
                await asyncio.wait_for(self._protocol_response.wait(), timeout=15)
            except asyncio.TimeoutError as error:
                raise FaceModeProtocolError("FaceMode did not acknowledge protocol negotiation") from error
            self._raise_protocol_error()
            if not self._protocol_started.is_set():
                raise FaceModeProtocolError("FaceMode protocol negotiation did not start")

    async def _send_control(self, message_type: str, sequence: int) -> None:
        self._raise_protocol_error()
        if self._websocket is None:
            if self._stopped:
                return
            raise FaceModeProtocolError("FaceMode WebSocket is not connected")
        async with self._send_lock:
            await self._websocket.send(json.dumps({"type": message_type, "seq": sequence}, separators=(",", ":")))

    async def _send_ping(self) -> None:
        self._raise_protocol_error()
        if self._websocket is None:
            raise FaceModeProtocolError("FaceMode WebSocket is not connected")
        async with self._send_lock:
            await self._websocket.send(json.dumps({
                "type": "ping",
                "ts": int(time.time() * 1000),
            }, separators=(",", ":")))

    async def _keepalive_loop(self) -> None:
        try:
            await self._protocol_response.wait()
            if not self._protocol_started.is_set() or self._protocol_error is not None:
                return
            while not self._stopped:
                await asyncio.sleep(15.0)
                if self._websocket is None or self._stopped:
                    return
                try:
                    await self._send_ping()
                except Exception as error:
                    if not self._stopped:
                        protocol_error = FaceModeProtocolError(
                            f"FaceMode keepalive failed: {error.__class__.__name__}"
                        )
                        self._set_protocol_error(protocol_error)
                        logger.warning("%s", protocol_error)
                    return
        except asyncio.CancelledError:
            raise

    async def _receive_events(self) -> None:
        try:
            async for raw in self._websocket:
                if isinstance(raw, bytes):
                    continue
                message = json.loads(raw)
                if not isinstance(message, Mapping):
                    raise FaceModeProtocolError("FaceMode WebSocket message was not an object")
                message_type = str(message.get("type", ""))
                if message_type == "started":
                    self._handle_started(message)
                elif message_type == "audio_ready":
                    self._avatar_ready.set()
                elif message_type == "utterance_ended":
                    logger.debug(
                        "FaceMode utterance ended seq=%s cycles=%s",
                        message.get("seq"),
                        message.get("cycles"),
                    )
                elif message_type == "pong":
                    logger.debug("FaceMode keepalive acknowledged")
                elif message_type == "session_ending":
                    protocol_error = FaceModeProtocolError(
                        f"FaceMode session is ending: {message.get('reason', 'unknown')}"
                    )
                    self._set_protocol_error(protocol_error)
                    logger.warning("%s", protocol_error)
                elif message_type == "ended":
                    self._session_ended.set()
                elif message_type == "error":
                    protocol_error = FaceModeProtocolError(
                        f"{message.get('code', 'INTERNAL_ERROR')}: {message.get('message', 'FaceMode error')}"
                    )
                    self._set_protocol_error(protocol_error)
                    logger.error("FaceMode protocol error: %s", protocol_error)
                    if bool(message.get("fatal", False)):
                        break
                else:
                    logger.debug("FaceMode WebSocket unhandled message type: %s", message_type)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            if not self._stopped:
                protocol_error = (
                    error
                    if isinstance(error, FaceModeProtocolError)
                    else FaceModeProtocolError(
                        "FaceMode WebSocket receiver stopped: "
                        f"{error.__class__.__name__}{_websocket_close_details(error)}"
                    )
                )
                self._set_protocol_error(protocol_error)
                logger.warning("%s", protocol_error)
        else:
            if not self._stopped and not self._session_ended.is_set():
                protocol_error = FaceModeProtocolError(
                    "FaceMode WebSocket closed unexpectedly"
                    f"{_websocket_close_details(self._websocket)}"
                )
                self._set_protocol_error(protocol_error)
                logger.warning("%s", protocol_error)

    def _handle_started(self, message: Mapping[str, Any]) -> None:
        if self.session is None:
            self._set_protocol_error(FaceModeProtocolError("FaceMode session is not initialized"))
            return
        if str(message.get("session_id", "")) != self.session.session_id:
            self._set_protocol_error(
                FaceModeProtocolError("FaceMode started response session ID did not match")
            )
            return
        if message.get("server_sample_rate") != 48_000 or message.get("server_channels") != 1:
            self._set_protocol_error(
                FaceModeProtocolError("FaceMode server reported an unsupported canonical audio format")
            )
            return
        self._protocol_started.set()
        self._protocol_response.set()
        logger.info("FaceMode protocol started session=%s", self._session_log_marker)

    async def _wait_for_video_track(self) -> None:
        room = self._room
        if room is None:
            raise FaceModeNotReadyError("LiveKit room is not available")
        if self._has_remote_video_track():
            return
        event = asyncio.Event()

        def on_track_subscribed(track, _publication, participant):
            if (
                track.kind == rtc.TrackKind.KIND_VIDEO
                and participant.identity == self.avatar_participant_identity
            ):
                event.set()

        room.on("track_subscribed", on_track_subscribed)
        try:
            if self._has_remote_video_track():
                return
            await event.wait()
        finally:
            room.off("track_subscribed", on_track_subscribed)

    def _has_remote_video_track(self) -> bool:
        if self._room is None:
            return False
        participant = self._room.remote_participants.get(self.avatar_participant_identity)
        if participant is None:
            return False
        return any(
            publication.track and publication.track.kind == rtc.TrackKind.KIND_VIDEO
            for publication in participant.track_publications.values()
        )

    def _schedule_control(self, coroutine, name: str) -> None:
        task = asyncio.create_task(coroutine, name=name)
        self._control_tasks.add(task)
        task.add_done_callback(self._control_task_done)

    def _control_task_done(self, task: asyncio.Task) -> None:
        self._control_tasks.discard(task)
        if task.cancelled():
            return
        error = task.exception()
        if error is not None and not self._stopped:
            protocol_error = (
                error
                if isinstance(error, FaceModeProtocolError)
                else FaceModeProtocolError(
                    f"FaceMode control message failed: {error.__class__.__name__}"
                )
            )
            self._set_protocol_error(protocol_error)
            logger.warning("%s", protocol_error)

    async def _drain_control_tasks(self) -> None:
        tasks = tuple(self._control_tasks)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._raise_protocol_error()

    async def _cancel_task(self, task: asyncio.Task | None) -> None:
        if task is None or task is asyncio.current_task():
            return
        if not task.done():
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    def _set_protocol_error(self, error: FaceModeProtocolError) -> None:
        if self._protocol_error is None:
            self._protocol_error = error
        self._protocol_response.set()

    def _raise_protocol_error(self) -> None:
        if self._protocol_error is not None:
            raise self._protocol_error

    def _next_sequence(self) -> int:
        sequence = self._sequence
        self._sequence += 1
        return sequence


def _token_identity_hint(token: str) -> str | None:
    claims = _token_claims_hint(token)
    identity = claims.get("sub") if claims else None
    return identity if isinstance(identity, str) and identity else None


def _token_room_hint(token: str) -> str:
    claims = _token_claims_hint(token)
    video = claims.get("video") if claims else None
    room = video.get("room") if isinstance(video, Mapping) else None
    return room if isinstance(room, str) and room else ""


def _token_claims_hint(token: str) -> Mapping[str, Any] | None:
    try:
        payload = token.split(".")[1]
        padding = "=" * (-len(payload) % 4)
        claims = json.loads(base64.urlsafe_b64decode(payload + padding))
    except (IndexError, ValueError, TypeError, json.JSONDecodeError):
        return None
    return claims if isinstance(claims, Mapping) else None


def _sanitize_session_marker(session_id: str) -> str:
    return "".join(
        char if char.isascii() and (char.isalnum() or char in "_-") else "_"
        for char in session_id
    )[:96]


def _websocket_close_details(value: Any) -> str:
    if value is None:
        return " (code=unknown)"
    received = getattr(value, "rcvd", None)
    code = getattr(value, "close_code", None) or getattr(value, "code", None)
    reason = getattr(value, "close_reason", None) or getattr(value, "reason", None)
    if received is not None:
        code = code or getattr(received, "code", None)
        reason = reason or getattr(received, "reason", None)
    reason_text = str(reason or "").strip()[:160]
    suffix = f", reason={reason_text}" if reason_text else ""
    return f" (code={code if code is not None else 'unknown'}{suffix})"
