# @facemode/agents-plugin-facemode

FaceMode avatar output for LiveKit Agents JavaScript applications.

## Install

```powershell
npm install @facemode/agents-plugin-facemode
```

From this repository:

```powershell
npm install
npm run build
```

## Usage

Pass a server-side LiveKit room object to `start()`. The room token must allow
the worker participant to join and publish audio/video in the customer room.
Keep the token server-side and never log it or expose it to browser clients.

```ts
import { AvatarSession } from '@facemode/agents-plugin-facemode';

const avatar = new AvatarSession({
  apiKey: process.env.FACEMODE_API_KEY!,
  avatarId: process.env.FACEMODE_AVATAR_ID,
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

## Room token grants

The room token must include room join, publish, and subscribe grants for the
worker participant. FaceMode does not create or replace the customer LiveKit
room. Keep the token server-side and do not expose it to browser clients.

## API URL

The default API URL is `https://api.facemode.io/api`. Set `apiUrl` for a
self-hosted or local backend.
