# livekit-plugins-facemode

FaceMode avatar output for LiveKit Agents Python applications.

## Install

```bash
pip install livekit-plugins-facemode
```

From this repository:

```bash
pip install .
```

Requires Python 3.13 or later, LiveKit 1.x, and LiveKit Agents 1.7 or later.

## Usage

The host application supplies a server-side LiveKit room object. The room token
must grant the FaceMode worker permission to join, publish, and subscribe in the
customer room. Keep the token on the server and never log it or expose it to a
browser client.

```python
import os

from livekit.agents import Agent, AgentSession, room_io
from livekit_plugins_facemode import AvatarSession

avatar = AvatarSession(
    api_key=os.environ["FACEMODE_API_KEY"],
    avatar_id=os.environ.get("FACEMODE_AVATAR_ID", ""),
)

await ctx.connect()
await avatar.start(
    agent_session,
    ctx.room,
    room={
        "type": "livekit",
        "url": os.environ["LIVEKIT_URL"],
        "token": os.environ["LIVEKIT_TOKEN"],
    },
)
await agent_session.start(
    agent=Agent(instructions="You are a helpful assistant."),
    room=ctx.room,
    room_options=room_io.RoomOptions(audio_output=False),
)
await avatar.wait_for_join()
```

`AvatarSession.start()` creates a FaceMode session with the room object. The create response normally returns `ready: true` with the ingestion `url` and a one-time `wsToken` (about 15 minute expiry), so no polling happens in the common path. If worker assignment is still pending, the plugin polls the session endpoint with bounded backoff for up to 240 seconds and stops immediately on `FAILED`/`ENDED` worker states. The initial response's room credentials remain in memory and are not expected in the polling response.

The ingestion WebSocket handshake uses the `Sec-WebSocket-Protocol` subprotocol `facemode.<wsToken>` with a 240 second open timeout. When the backend returns optional `ingestion.headers`, the plugin forwards them during the handshake. It disables compression and native WebSocket keepalives in favor of the canonical application `ping`/`pong` messages after negotiation.

Pass `input_provider` (one of `deepgram`, `gemini`, `gnani`, `elevenlabs`, `openai`, `cartesia`, `sarvam`, `custom`) to `AvatarSession` when the session should pin a specific upstream input provider; it is omitted from the request otherwise.

If the WebSocket transport drops after the protocol has started, the plugin auto-reconnects in the background: it POSTs the session reconnect endpoint for a fresh one-time `wsToken` (a consumed token is never reused), opens a new socket with `facemode.<newToken>`, re-sends the canonical `start` message, and waits for `started` before resuming. Outgoing control `seq` numbers stay monotonic across the reconnect and buffered audio is not replayed. Reconnect is bounded to 2 attempts with backoff and is single-flight; exhaustion surfaces a typed `FaceModeNotReadyError`/`FaceModeAPIError`. Intentional `stop()`, a canonical `ended`, fatal protocol errors, and drops before the first negotiation do not reconnect.

Every TTS frame is sent as canonical PCM audio. The plugin locks sample rate and channel count at protocol negotiation and rejects a format change or unaligned 16-bit PCM afterward. `cancel_utterance` is sent when LiveKit interrupts the audio output. Graceful close sends `end_session` and waits briefly for canonical `ended`; `ended` is a protocol acknowledgement, not backend cleanup confirmation.

## Token grants

The `room.token` value must allow the worker to join the customer room and
publish its avatar audio/video tracks. If the token uses a fixed participant
identity, use `avatar_participant_identity` and make the room policy allow that
identity. The worker does not create or replace the customer LiveKit room.

## API URL

The default API URL is `https://api.facemode.io/api`. Set `api_url` for a
self-hosted or local backend.

## License

Apache-2.0 - see [LICENSE](LICENSE). Part of the [facemode-integrations](https://github.com/facemode-by-infinitylevel/facemode-integrations) monorepo.
