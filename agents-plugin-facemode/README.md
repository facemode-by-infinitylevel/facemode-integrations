# @facemode/agents-plugin-facemode

FaceMode avatar output for LiveKit Agents JavaScript applications.

## Install

```bash
npm install @facemode/agents-plugin-facemode
```

From this repository:

```bash
npm install
npm run build
```

Requires Node.js 20 or later and `@livekit/agents` 1.x.

## Usage

Pass a server-side LiveKit room object to `start()`. The room token must allow
the worker participant to join and publish audio/video in the customer room.
Keep the token server-side and never log it or expose it to browser clients.

```ts
import { AvatarSession } from '@facemode/agents-plugin-facemode';

const avatar = new AvatarSession({
  apiKey: process.env.FACEMODE_API_KEY!,
  avatarId: process.env.FACEMODE_AVATAR_ID,
  inputProvider: 'deepgram', // optional session input provider
});

await avatar.start(agentSession, ctx.room, {
  room: {
    type: 'livekit',
    url: process.env.LIVEKIT_URL!,
    token: process.env.LIVEKIT_TOKEN!,
  },
});
await avatar.waitForJoin();
```

The plugin creates a FaceMode session through `POST /api/sessions` with the
room object, negotiates the canonical WebSocket protocol, and replaces the
AgentSession audio tail. TTS frames are forwarded as binary `pcm_s16le`;
interruptions send `cancel_utterance`.

The plugin accepts integer PCM sample rates from 8kHz through 48kHz. Preferred
rates covering common TTS outputs are 8, 11.025, 12, 16, 22.05, 24, 32, 44.1,
and 48kHz. Other in-range rates are accepted and logged once during protocol
negotiation; the declared rate must match the actual PCM data.

## Session creation

`POST /api/sessions` returns `201` immediately with `ready: true`, the worker
WebSocket `url`, and a one-time `wsToken` (about 15 minutes). The plugin still
polls `GET /api/sessions/{id}` as a guard if a backend ever reports
`ready: false`, but an already-ready create response skips polling entirely.
`FAILED` and `ENDED` worker states short-circuit immediately.

## Worker WebSocket

The worker connection authenticates with the `facemode.<wsToken>` WebSocket
subprotocol; each token is single-use. The plugin sends `start` and waits for
`started` before streaming audio. Both the ingestion readiness budget and the
WebSocket handshake budget are 240 seconds.

## inputProvider

`new AvatarSession({ inputProvider })` optionally selects the session input
provider. Supported values: `deepgram`, `gemini`, `gnani`, `elevenlabs`,
`openai`, `cartesia`, `sarvam`, `custom`. It is sent on session creation only
when set.

## Auto-reconnect

If the worker transport drops after `started`, the plugin posts to
`/api/sessions/{id}/reconnect`, receives a fresh one-time `wsToken`, opens a
new socket with `facemode.<newToken>`, re-sends the identical `start` message,
and waits for `started` before resuming. At most one reconnect runs at a time,
with up to 2 attempts and a short backoff; tokens are never reused. Session
identity and outgoing sequence numbers are preserved. Sends made while the
transport is down wait for the barrier and are delivered once on the new
socket - nothing already sent is replayed.

No reconnect is attempted after `stop()`, after the worker reports `ended`, or
after a fatal protocol error. If all attempts fail, pending and future sends
raise `FaceModeProtocolError`.

## Room token grants

The room token must include room join, publish, and subscribe grants for the
worker participant. FaceMode does not create or replace the customer LiveKit
room. Keep the token server-side and do not expose it to browser clients.

## API URL

The default API URL is `https://api.facemode.io/api`. Set `apiUrl` for a
self-hosted or local backend.

## License

Apache-2.0 - see [LICENSE](LICENSE). Part of the [facemode-integrations](https://github.com/facemode-by-infinitylevel/facemode-integrations) monorepo.
