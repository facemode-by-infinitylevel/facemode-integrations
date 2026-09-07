import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import WebSocket, { WebSocketServer } from 'ws';

import { AvatarSession } from '../dist/avatar.js';
import { parseSessionDetails } from '../dist/models.js';

async function waitForListening(server) {
  if (server.address()) return;
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.once('listening', () => {
      server.off('error', reject);
      resolve();
    });
  });
}

async function close(server) {
  await new Promise((resolve, reject) => {
    server.close((error) => error ? reject(error) : resolve());
  });
}

test('rejects an unaligned PCM frame after format negotiation', async () => {
  const avatar = Object.create(AvatarSession.prototype);
  avatar.websocket = {
    readyState: WebSocket.OPEN,
    send() {},
  };
  avatar.inputFormat = { sampleRate: 24000, channels: 2 };

  await assert.rejects(
    avatar.sendAudioFrame({
      sampleRate: 24000,
      channels: 2,
      data: new Uint8Array([1, 2, 3]),
    }),
    /not aligned to 2 channel 16-bit samples/,
  );
});

test('polls pending ingestion until a ready assignment preserves room credentials', async () => {
  const originalFetch = globalThis.fetch;
  const responses = [
    {
      sessionId: 'session-123',
      workerStatus: 'ASSIGNING',
      ingestion: { ready: false, authority: 'control' },
    },
    {
      sessionId: 'session-123',
      workerStatus: 'ASSIGNED',
      ingestion: {
        ready: true,
        authority: 'control',
        url: 'wss://worker.example.test/ws/session-123',
        wsToken: 'opaque-websocket-token',
      },
    },
  ];
  globalThis.fetch = async () => new Response(JSON.stringify(responses.shift()), {
    headers: { 'Content-Type': 'application/json' },
  });

  try {
    const avatar = Object.create(AvatarSession.prototype);
    avatar.apiKey = 'api-key';
    avatar.apiUrl = 'https://api.example.test/api';
    const initial = parseSessionDetails({
      sessionId: 'session-123',
      room: {
        type: 'livekit',
        url: 'wss://livekit.example.test',
        token: 'runtime-room-token',
        name: 'room-123',
      },
      ingestion: { ready: false, authority: 'control' },
    });

    const ready = await avatar.waitForIngestion(initial);
    assert.equal(ready.ingestion.ready, true);
    assert.equal(ready.ingestion.wsToken, 'opaque-websocket-token');
    assert.equal(ready.room.token, 'runtime-room-token');
  } finally {
    globalThis.fetch = originalFetch;
  }
});


test('does not surface remote WebSocket close reasons', () => {
  const remoteMarker = 'remote-close-credential-marker';
  const avatar = Object.create(AvatarSession.prototype);
  avatar.stopped = false;
  avatar.sessionLogMarker = 'session-123';
  avatar.stopApplicationKeepalive = () => {};
  avatar.setProtocolError = (error) => {
    avatar.protocolError = error;
  };
  const socket = new EventEmitter();

  const loggerKey = Symbol.for('@livekit/agents:logger');
  const originalLogger = globalThis[loggerKey];
  globalThis[loggerKey] = { warn() {} };
  try {
    avatar.bindWebSocketEvents(socket);
    socket.emit('close', 4001, Buffer.from(remoteMarker));
  } finally {
    if (originalLogger === undefined) {
      delete globalThis[loggerKey];
    } else {
      globalThis[loggerKey] = originalLogger;
    }
  }

  assert.match(avatar.protocolError.message, /code=4001/);
  assert.equal(avatar.protocolError.message.includes(remoteMarker), false);
});


test('opens canonical WebSocket with strict affinity and disabled compression', async () => {
  const server = new WebSocketServer({
    host: '127.0.0.1',
    port: 0,
    perMessageDeflate: false,
  });
  await waitForListening(server);
  const headers = new Promise((resolve, reject) => {
    server.once('error', reject);
    server.once('connection', (_socket, request) => resolve(request.headers));
  });

  try {
    const address = server.address();
    assert.ok(address && typeof address === 'object');
    const avatar = Object.create(AvatarSession.prototype);
    const socketPromise = avatar.connectWebSocket(
      `ws://127.0.0.1:${address.port}/ws/session-123`,
      'one-time-token',
      { 'X-Runpod-Worker-Id': 'strict worker-123' },
    );
    const [socket, requestHeaders] = await Promise.all([socketPromise, headers]);

    assert.equal(requestHeaders['x-runpod-worker-id'], 'strict worker-123');
    assert.equal(requestHeaders['sec-websocket-protocol'], 'aivatar.one-time-token');
    assert.equal(requestHeaders['sec-websocket-extensions'], undefined);
    socket.close();
  } finally {
    await close(server);
  }
});
