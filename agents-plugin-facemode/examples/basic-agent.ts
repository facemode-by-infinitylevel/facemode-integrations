import { fileURLToPath } from 'node:url';
import {
  ServerOptions,
  cli,
  defineAgent,
  inference,
  voice,
} from '@livekit/agents';
import { AvatarSession } from '@facemode/agents-plugin-facemode';

export default defineAgent({
  entry: async (ctx) => {
    const session = new voice.AgentSession({
      stt: new inference.STT({ model: 'deepgram/nova-3', language: 'multi' }),
      llm: new inference.LLM({ model: 'openai/gpt-5.4-mini' }),
      tts: new inference.TTS({
        model: 'cartesia/sonic-3',
        voice: '9626c31c-bec5-4cca-baa8-f8ba9e84c8bc',
      }),
    });
    const avatar = new AvatarSession({
      apiKey: process.env.FACEMODE_API_KEY ?? '',
      avatarId: process.env.FACEMODE_AVATAR_ID,
      apiUrl: process.env.FACEMODE_API_URL,
    });
    await ctx.connect();
    await avatar.start(session, ctx.room, {
      room: {
        type: 'livekit',
        url: process.env.LIVEKIT_URL ?? '',
        token: process.env.FACEMODE_ROOM_TOKEN ?? process.env.LIVEKIT_WORKER_TOKEN ?? process.env.LIVEKIT_TOKEN ?? '',
        name: process.env.LIVEKIT_ROOM_NAME,
      },
    });
    await session.start({
      agent: new voice.Agent({ instructions: 'You are a helpful voice assistant.' }),
      room: ctx.room,
      outputOptions: { audioEnabled: false },
    });
    await session.generateReply({ instructions: 'Greet the user and ask how you can help in 2-3 sentences.' });
  },
});

cli.runApp(new ServerOptions({
  agent: fileURLToPath(import.meta.url),
  agentName: 'facemode-basic',
}));
