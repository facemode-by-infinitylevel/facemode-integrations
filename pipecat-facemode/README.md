# pipecat-facemode

FaceMode avatar output for Pipecat pipelines. The integration taps
`TTSAudioRawFrame`, sends canonical 16-bit PCM to FaceMode, and converts the
avatar's LiveKit audio and video tracks back into Pipecat output frames.

The package is intentionally a small `FrameProcessor`. It does not replace a
Pipecat transport. Place it after a TTS service and before the output transport:

```text
transport input -> STT -> LLM -> TTS -> FaceModeVideoService -> transport output
```

By default, source `TTSAudioRawFrame` objects are consumed. This prevents the
source TTS audio and the avatar's returned audio from being played twice. Set
`forward_tts_audio=True` only when the pipeline intentionally needs both.

## Install

```powershell
pip install pipecat-facemode
```

From this directory:

```powershell
pip install .
```

The package supports Pipecat 1.7 through the current 1.x line, LiveKit RTC 1.x, aiohttp 3.x, websockets 14 through 16, and NumPy 2.x.

## Basic usage

`room.token` is the server-side worker token sent to FaceMode. The separate
`livekit_subscriber_token` is used by this Pipecat process to subscribe to the
room. Use separate participant identities and tokens in production.

```python
import os

from pipecat.pipeline.pipeline import Pipeline
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat_facemode import FaceModeVideoService


tts = CartesiaTTSService(
    api_key=os.environ["CARTESIA_API_KEY"],
    settings=CartesiaTTSService.Settings(
        voice="71a7ad14-091c-4e8e-a314-022ece01c121",
    ),
)

facemode = FaceModeVideoService(
    api_key=os.environ["FACEMODE_API_KEY"],
    avatar_id=os.environ.get("FACEMODE_AVATAR_ID", ""),
    room={
        "type": "livekit",
        "url": os.environ["LIVEKIT_URL"],
        "token": os.environ["LIVEKIT_WORKER_TOKEN"],
    },
    livekit_subscriber_token=os.environ["LIVEKIT_SUBSCRIBER_TOKEN"],
    room_name=os.environ["LIVEKIT_ROOM_NAME"],
    # Optional speech input provider persisted by the backend for the session:
    # deepgram, gemini, gnani, elevenlabs, openai, cartesia, sarvam, or custom.
    input_provider=os.environ.get("FACEMODE_INPUT_PROVIDER") or None,
)

pipeline = Pipeline([
    transport.input(),
    tts,
    facemode,
    transport.output(),
])
```

See [`examples/basic_pipeline.py`](examples/basic_pipeline.py) for a complete
pipeline using Pipecat's LiveKit transport. The same-room example disables the
transport's audio and video outputs because the FaceMode worker already publishes
both avatar tracks directly into that LiveKit room. It also uses distinct tokens
for the FaceMode worker, Pipecat subscriber, Pipecat transport, and viewer.

## FaceMode session and WebSocket protocol

On `StartFrame`, the service:

1. Connects to the supplied LiveKit room with `livekit-rtc` and enables automatic
   track subscription.
2. Calls `POST {api_url}/sessions` with the room object (and `inputProvider`
   when `input_provider` is configured). The request never sends
   `waitForIngestion`; current backends reject it as an unknown field:

   ```json
   {
     "avatarId": "avatar-id",
     "inputProvider": "deepgram",
     "room": {
       "type": "livekit",
       "url": "wss://project.livekit.cloud",
       "token": "server-side-token"
     }
   }
   ```

   A `201` response returns `ingestion.ready: true` with the worker WebSocket
   `url` and a one-time `wsToken` immediately.

3. When the response has `ingestion.ready: false`, polls `GET {api_url}/sessions/{id}` using bounded backoff for up to 240 seconds until ready WebSocket credentials are available; `FAILED` or `ENDED` worker states stop the wait immediately. Room credentials from the initial response remain in memory and are not expected in the status response.
4. Opens the returned WebSocket with the `facemode.<ws-token>` subprotocol (the `aivatar.` prefix is rejected by current backends), compression disabled, native keepalives disabled, a 240-second open timeout, and optional backend-provided `ingestion.headers` forwarded unchanged. This package requires `websockets>=14`, so it uses the `additional_headers` API and never falls back to an unaffinitized connection.
5. Starts the receive task, sends canonical `start` from the pipeline `StartFrame`, and waits for a validated `started` response.
6. Starts application keepalives only after negotiation succeeds, preserving the protocol requirement that `start` is the first application message.

Call `await service.wait_for_avatar()` when an application needs to wait for
FaceMode's `audio_ready` event or the first LiveKit video frame before announcing
that avatar media is available.

The canonical protocol messages are used as follows:

- `start` negotiates `audio_encoding: pcm_s16le`, sample rate, and channel count.
- `start_utterance` precedes the first binary audio chunk.
- `TTSAudioRawFrame.audio` is sent as binary little-endian signed 16-bit PCM,
  with no JSON or base64 wrapper.
- `end_utterance` follows a TTS stop frame or a short idle period.
- `cancel_utterance` is sent for `StartInterruptionFrame`.
- `ping` is sent periodically while the session is alive.
- `end_session` is sent during graceful `EndFrame` or pipeline cleanup.

## Automatic reconnect

If the canonical WebSocket drops after a session was fully started, the service
reconnects without caller-visible pipeline failure:

1. It calls `POST {api_url}/sessions/{id}/reconnect` with the original `room`
   object (plus the persisted `inputProvider` when configured). Every response
   mints a fresh one-time `wsToken`; tokens are never reused.
2. It opens a new socket with the `facemode.<new-token>` subprotocol, resends
   the same `start` negotiation, and waits for `started` before audio resumes.
3. At most 2 reconnect attempts run with bounded backoff. Recovery is
   single-flight, so simultaneous close/error notifications share one attempt.

Sequence numbers keep increasing across reconnects, an explicitly open
utterance is re-declared on the new socket, and already-sent audio is never
replayed. Sends that arrive during the gap wait for the new socket instead of
failing. No reconnect is attempted on intentional shutdown (`EndFrame`,
`CancelFrame`, `stop`, `cleanup`), on server-terminal states (`session_ending`,
`ended`, fatal `error` messages), or before the session is fully started. When
the reconnect budget is exhausted, the failure surfaces as a
`FaceModeProtocolError` that never contains token or key material.

The service calls `super().__init__()` and `super().process_frame(...)`, and
forwards non-consumed frames with their original `FrameDirection`. Lifecycle and
interruption frames are forwarded after the FaceMode action completes. Pipecat
1.7 exposes the interruption event as `InterruptionFrame`; releases that expose
separate `StartInterruptionFrame` and `StopInterruptionFrame` classes are handled
without changing the public service API.

## LiveKit token grants

Keep all LiveKit and FaceMode credentials on the server. Never put them in
browser code or log messages.

The token in `room.token` must identify the FaceMode worker participant and grant
that participant:

- `room_join=True` for the exact `LIVEKIT_ROOM_NAME`;
- `can_publish=True` so the avatar can publish its audio and video tracks;
- `can_subscribe=True` so the worker can join the room's media session; and
- a stable, unique participant identity and name if your room policy requires it.

The separate `livekit_subscriber_token` used by `FaceModeVideoService` must
identify the Pipecat subscriber and grant:

- `room_join=True` for the same room; and
- `can_subscribe=True`.

Grant subscriber publish permission only if the application needs it. A token
with publish and subscribe grants can be reused for both connections, but sharing
a participant identity between the FaceMode worker and Pipecat subscriber is not
recommended because a LiveKit room permits only one participant for an identity.

Example server-side token generation with `livekit-api`:

```python
from livekit import api

worker_token = (
    api.AccessToken()
    .with_identity("facemode-avatar")
    .with_name("FaceMode Avatar")
    .with_grants(
        api.VideoGrants(
            room_join=True,
            room=room_name,
            can_publish=True,
            can_subscribe=True,
        )
    )
    .to_jwt()
)

subscriber_token = (
    api.AccessToken()
    .with_identity("pipecat-facemode-subscriber")
    .with_name("Pipecat FaceMode Subscriber")
    .with_grants(
        api.VideoGrants(
            room_join=True,
            room=room_name,
            can_subscribe=True,
        )
    )
    .to_jwt()
)
```

The sample token code is for a backend only. The LiveKit API secret must never be
sent to a client.

## Video frame compatibility

Pipecat 1.7 uses `OutputImageRawFrame` for output video. The package also checks
for `OutputVideoRawFrame` and falls back to `ImageRawFrame`, so importing the
package remains safe across Pipecat releases that use different output video
class names. LiveKit video is requested as RGB24 when the SDK exposes that
format, then copied into a NumPy-backed RGB or RGBA image frame.

## Lifecycle and failure behavior

- REST and WebSocket failures raise typed FaceMode exceptions without including
  API keys or token values in log messages; `facemode.` credential prefixes are
  redacted alongside `Bearer` material.
- The ingestion-ready poll budget and the WebSocket open/handshake budget are
  each 240 seconds. The `start`/`started` acknowledgement uses
  `protocol_timeout` (default 15 seconds).
- WebSocket receive, keepalive, audio, and video tasks are cancelled and awaited during `EndFrame`, `cleanup`, or a failed startup. Graceful shutdown sends `end_session` and waits up to three seconds for canonical `ended` before closing the socket.
- `ended` is a WebSocket protocol acknowledgement, not proof that backend worker cleanup, provider cancellation, or billing finalization is complete. Callers that need backend cleanup confirmation poll the public session endpoint until it becomes terminal.
- LiveKit tracks published before the connection are discovered after startup;
  later `track_subscribed` events are handled automatically.
- `avatar_participant_identity` defaults to `facemode-avatar` to avoid forwarding
  other remote participants. Set it to the identity in the worker room token when
  that token uses a different identity. The local subscriber track is always
  ignored.
