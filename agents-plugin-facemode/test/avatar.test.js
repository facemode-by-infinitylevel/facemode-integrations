import assert from 'node:assert/strict';
import { EventEmitter } from 'node:events';
import test from 'node:test';
import WebSocket, { WebSocketServer } from 'ws';

import * as avatarModule from '../dist/avatar.js';
import { AvatarSession } from '../dist/avatar.js';
import { FaceModeApiError, FaceModeProtocolError } from '../dist/exceptions.js';
import { parseSessionDetails } from '../dist/models.js';

const loggerKey = Symbol.for('@livekit/agents:logger');
if (!globalThis[loggerKey]) {
  globalThis[loggerKey] = { debug() {}, info() {}, warn() {}, error() {} };
}

const ROOM = {
  type: 'livekit',
  url: 'wss://livekit.example.test',
  token: 'room-token',
  name: 'room-123',
};

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

function testDeferred() {
  let resolve;
  let reject;
  const promise = new Promise((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { resolve, reject, promise };
}

async function waitFor(condition, timeoutMs = 8000) {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) {
      throw new Error('waitFor condition timed out');
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}

async function delay(milliseconds) {
  await new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function makeSessionDetails(overrides = {}) {
  return parseSessionDetails({
    sessionId: 'session-123',
    room: ROOM,
    workerStatus: 'ACTIVE',
    ingestion: {
      ready: true,
      url: 'wss://worker-1.example.test/ws/session-123',
      wsToken: 'token-1',
    },
    ...overrides,
  });
}

function makeAvatar(overrides = {}) {
  const avatar = Object.create(AvatarSession.prototype);
  avatar.apiKey = 'api-key';
  avatar.apiUrl = 'https://api.example.test/api';
  avatar.avatarId = 'avatar-1';
  avatar.roomConfig = { ...ROOM };
  avatar.session = makeSessionDetails();
  avatar.inputFormat = { sampleRate: 24000, channels: 2 };
  avatar.sequence = 0;
  avatar.stopped = false;
  avatar.sessionTerminated = false;
  avatar.reconnectPromise = null;
  avatar.protocolError = null;
  avatar.protocolStartedAck = false;
  avatar.protocolStarted = null;
  avatar.sessionEnded = null;
  avatar.keepaliveTimer = null;
  avatar.websocket = null;
  avatar.sessionLogMarker = 'session-123';
  avatar.avatarVideoResolve = null;
  avatar.outputSlot = null;
  avatar.inputProvider = undefined;
  Object.assign(avatar, overrides);
  return avatar;
}

function cleanupAvatar(avatar) {
  avatar.stopped = true;
  try {
    avatar.stopApplicationKeepalive();
  } catch {
    // defensive cleanup only
  }
  const socket = avatar.websocket;
  if (socket) {
    avatar.websocket = null;
    try {
      if (typeof avatar.teardownSocket === 'function') {
        avatar.teardownSocket(socket);
      } else {
        socket.removeAllListeners();
        socket.terminate();
      }
    } catch {
      // defensive cleanup only
    }
  }
}

function stubFetch(handler) {
  const calls = [];
  const original = globalThis.fetch;
  globalThis.fetch = async (url, init) => {
    calls.push({ url: String(url), init });
    return handler(String(url), init, calls.length);
  };
  return {
    calls,
    restore() {
      globalThis.fetch = original;
    },
  };
}

class FakeWorker {
  constructor({ autoStart = true, closeOnConnect = false } = {}) {
    this.autoStart = autoStart;
    this.closeOnConnect = closeOnConnect;
    this.subprotocols = [];
    this.textMessages = [];
    this.binaryMessages = [];
    this.sockets = new Set();
    this.server = null;
    this.url = '';
  }

  async listen() {
    this.server = new WebSocketServer({
      host: '127.0.0.1',
      port: 0,
      perMessageDeflate: false,
    });
    this.server.on('connection', (socket, request) => {
      this.subprotocols.push(request.headers['sec-websocket-protocol']);
      this.sockets.add(socket);
      socket.on('message', (data, isBinary) => this.handleMessage(socket, data, isBinary));
      socket.on('close', () => this.sockets.delete(socket));
      if (this.closeOnConnect) {
        socket.terminate();
      }
    });
    await waitForListening(this.server);
    const address = this.server.address();
    assert.ok(address && typeof address === 'object');
    this.url = `ws://127.0.0.1:${address.port}/ws/session-123`;
  }

  handleMessage(socket, data, isBinary) {
    if (isBinary) {
      this.binaryMessages.push(Buffer.from(data));
      return;
    }
    const text = data.toString();
    this.textMessages.push(text);
    let message = null;
    try {
      message = JSON.parse(text);
    } catch {
      message = null;
    }
    if (message && message.type === 'start' && this.autoStart) {
      socket.send(JSON.stringify({
        type: 'started',
        session_id: message.session_id,
        server_sample_rate: 48000,
        server_channels: 1,
      }));
    }
  }

  parsed(type) {
    return this.textMessages
      .map((raw) => JSON.parse(raw))
      .filter((message) => message.type === type);
  }

  terminateAll() {
    for (const socket of this.sockets) {
      try {
        socket.terminate();
      } catch {
        // best-effort teardown of the fake worker side
      }
    }
  }

  async close() {
    this.terminateAll();
    if (this.server) await close(this.server);
  }
}

async function connectAndStart(avatar, worker, token = 'token-1') {
  const socket = await avatar.connectWebSocket(worker.url, token);
  avatar.websocket = socket;
  avatar.bindWebSocketEvents(socket);
  avatar.protocolStarted = testDeferred();
  avatar.sessionEnded = testDeferred();
  avatar.startApplicationKeepalive();
  await avatar.ensureProtocolStarted(24000, 2);
  return socket;
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

test('uses 240 second ingestion and handshake budgets', () => {
  assert.equal(avatarModule.INGESTION_READY_TIMEOUT_MS, 240000);
  assert.equal(avatarModule.WS_HANDSHAKE_TIMEOUT_MS, 240000);
});

test('create session payload drops waitForIngestion and sends inputProvider', async () => {
  const stub = stubFetch(() => new Response(JSON.stringify({
    sessionId: 'session-123',
    room: ROOM,
    ingestion: { ready: true, url: 'wss://worker-1.example.test/ws/session-123', wsToken: 'token-1' },
  }), { status: 201, headers: { 'Content-Type': 'application/json' } }));

  try {
    const withProvider = new AvatarSession({
      apiKey: 'api-key',
      avatarId: 'avatar-1',
      apiUrl: 'https://api.example.test/api',
      inputProvider: 'gemini',
    });
    await withProvider.createSession(ROOM, 'room-123');
    assert.equal(stub.calls.length, 1);
    const body = JSON.parse(stub.calls[0].init.body);
    assert.equal('waitForIngestion' in body, false);
    assert.equal(body.inputProvider, 'gemini');
    assert.equal(body.avatarId, 'avatar-1');
    assert.deepEqual(body.room, ROOM);
    assert.equal(body.livekit_room_id, 'room-123');

    const plain = makeAvatar();
    await plain.createSession(ROOM, 'room-123');
    const plainBody = JSON.parse(stub.calls[1].init.body);
    assert.equal('waitForIngestion' in plainBody, false);
    assert.equal('inputProvider' in plainBody, false);
  } finally {
    stub.restore();
  }
});

test('short-circuits ingestion polling when the session is already ready', async () => {
  const avatar = makeAvatar();
  const stub = stubFetch(() => new Response('{}', { status: 500 }));
  try {
    const ready = makeSessionDetails();
    const result = await avatar.waitForIngestion(ready);
    assert.equal(result.ingestion.ready, true);
    assert.equal(result.ingestion.wsToken, 'token-1');
    assert.equal(stub.calls.length, 0);
  } finally {
    stub.restore();
  }
});

test('short-circuits ingestion polling on a terminal worker status', async () => {
  const avatar = makeAvatar();
  const stub = stubFetch(() => new Response('{}', { status: 500 }));
  try {
    const failed = makeSessionDetails({
      workerStatus: 'FAILED',
      ingestion: { ready: false },
    });
    await assert.rejects(
      avatar.waitForIngestion(failed),
      (error) => error instanceof FaceModeApiError && /failed state/.test(error.message),
    );
    assert.equal(stub.calls.length, 0);
  } finally {
    stub.restore();
  }
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
  avatar.sessionTerminated = false;
  avatar.sessionLogMarker = 'session-123';
  avatar.stopApplicationKeepalive = () => {};
  avatar.setProtocolError = (error) => {
    avatar.protocolError = error;
  };
  const socket = new EventEmitter();
  avatar.websocket = socket;

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

  assert.equal(avatar.websocket, null);
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
    assert.equal(requestHeaders['sec-websocket-protocol'], 'facemode.one-time-token');
    assert.equal(requestHeaders['sec-websocket-extensions'], undefined);
    socket.close();
  } finally {
    await close(server);
  }
});

test('reconnects on transport loss with a fresh one-time token and resumes the session', async () => {
  const worker1 = new FakeWorker();
  const worker2 = new FakeWorker();
  await worker1.listen();
  await worker2.listen();

  const avatar = makeAvatar();
  const stub = stubFetch((url) => {
    if (url.endsWith('/sessions/session-123/reconnect')) {
      return new Response(JSON.stringify({
        room: ROOM,
        workerStatus: 'ACTIVE',
        ingestion: { ready: true, url: worker2.url, wsToken: 'token-2' },
      }), { status: 200, headers: { 'Content-Type': 'application/json' } });
    }
    return new Response('{}', { status: 404 });
  });

  try {
    const deadSocket = await connectAndStart(avatar, worker1);
    assert.equal(worker1.subprotocols[0], 'facemode.token-1');
    assert.equal(avatar.protocolStartedAck, true);
    const keepaliveBefore = avatar.keepaliveTimer;
    assert.ok(keepaliveBefore);

    await avatar.sendControl('start_utterance', avatar.nextSequence());
    await avatar.sendControl('end_utterance', avatar.nextSequence());
    await avatar.sendAudioFrame({ sampleRate: 24000, channels: 2, data: new Int16Array(960) });
    await waitFor(() => worker1.binaryMessages.length === 1);
    assert.deepEqual(
      worker1.parsed('start_utterance').map((message) => message.seq),
      [0],
    );

    worker1.terminateAll();
    const inFlightFrame = avatar.sendAudioFrame({
      sampleRate: 24000,
      channels: 2,
      data: new Int16Array(480),
    });
    const extraFlight = avatar.reconnect();
    await waitFor(() => (
      avatar.websocket !== null
      && avatar.websocket !== deadSocket
      && avatar.protocolStartedAck === true
      && stub.calls.length === 1
    ));
    await extraFlight;
    await inFlightFrame;

    assert.equal(stub.calls.length, 1);
    assert.equal(stub.calls[0].url, 'https://api.example.test/api/sessions/session-123/reconnect');
    assert.equal(stub.calls[0].init.method, 'POST');
    assert.equal(stub.calls[0].init.headers.Authorization, 'Bearer api-key');
    assert.deepEqual(JSON.parse(stub.calls[0].init.body), { room: ROOM });

    assert.equal(worker2.subprotocols.length, 1);
    assert.equal(worker2.subprotocols[0], 'facemode.token-2');
    assert.notEqual(worker2.subprotocols[0], worker1.subprotocols[0]);

    const starts1 = worker1.parsed('start');
    const starts2 = worker2.parsed('start');
    assert.equal(starts2.length, 1);
    assert.deepEqual(starts2[0], starts1[0]);

    assert.equal(deadSocket.listenerCount('message'), 0);
    assert.equal(deadSocket.listenerCount('close'), 0);
    assert.equal(deadSocket.listenerCount('error'), 0);
    assert.ok(avatar.keepaliveTimer);
    assert.notEqual(avatar.keepaliveTimer, keepaliveBefore);

    // the frame held by the send barrier lands exactly once; nothing is replayed
    await waitFor(() => worker2.binaryMessages.length === 1);
    assert.equal(worker1.binaryMessages.length, 1);

    await avatar.sendControl('start_utterance', avatar.nextSequence());
    const controls2 = worker2.parsed('start_utterance');
    assert.equal(controls2.length, 1);
    assert.equal(controls2[0].seq, 2);

    await avatar.sendAudioFrame({ sampleRate: 24000, channels: 2, data: new Int16Array(960) });
    await waitFor(() => worker2.binaryMessages.length === 2);
    assert.equal(avatar.session.sessionId, 'session-123');
  } finally {
    stub.restore();
    cleanupAvatar(avatar);
    await worker1.close();
    await worker2.close();
  }
});

test('keeps at most one reconnect in flight', async () => {
  const worker = new FakeWorker();
  await worker.listen();

  const avatar = makeAvatar();
  let releaseFetch;
  const gate = new Promise((resolve) => {
    releaseFetch = resolve;
  });
  const stub = stubFetch(async () => {
    await gate;
    return new Response(JSON.stringify({
      room: ROOM,
      workerStatus: 'ACTIVE',
      ingestion: { ready: true, url: worker.url, wsToken: 'token-2' },
    }), { status: 200, headers: { 'Content-Type': 'application/json' } });
  });

  try {
    const first = avatar.reconnect();
    const second = avatar.reconnect();
    assert.equal(first, second);
    releaseFetch();
    await first;
    assert.equal(stub.calls.length, 1);
    assert.equal(worker.subprotocols.length, 1);
    assert.equal(worker.subprotocols[0], 'facemode.token-2');
    assert.equal(avatar.protocolStartedAck, true);
  } finally {
    stub.restore();
    cleanupAvatar(avatar);
    await worker.close();
  }
});

test('reconnect exhaustion surfaces a typed error and never reuses tokens', async () => {
  const worker1 = new FakeWorker();
  const worker2 = new FakeWorker({ autoStart: false, closeOnConnect: true });
  await worker1.listen();
  await worker2.listen();

  const avatar = makeAvatar();
  const stub = stubFetch((url, _init, count) => new Response(JSON.stringify({
    room: ROOM,
    workerStatus: 'ACTIVE',
    ingestion: { ready: true, url: worker2.url, wsToken: `reconnect-token-${count}` },
  }), { status: 200, headers: { 'Content-Type': 'application/json' } }));

  try {
    const deadSocket = await connectAndStart(avatar, worker1);
    worker1.terminateAll();
    await waitFor(() => avatar.protocolError !== null && avatar.reconnectPromise === null);

    assert.ok(avatar.protocolError instanceof FaceModeProtocolError);
    assert.equal(stub.calls.length, 2);
    assert.deepEqual(worker2.subprotocols, [
      'facemode.reconnect-token-1',
      'facemode.reconnect-token-2',
    ]);
    assert.equal(new Set(worker2.subprotocols).size, 2);
    assert.equal(avatar.websocket, null);
    assert.equal(deadSocket.listenerCount('message'), 0);
    assert.equal(deadSocket.listenerCount('close'), 0);
  } finally {
    stub.restore();
    cleanupAvatar(avatar);
    await worker1.close();
    await worker2.close();
  }
});

test('does not reconnect on intentional close', async () => {
  const worker = new FakeWorker();
  await worker.listen();

  const avatar = makeAvatar();
  const stub = stubFetch(() => new Response('{}', { status: 500 }));

  try {
    await connectAndStart(avatar, worker);
    avatar.stopped = true;
    worker.terminateAll();
    await delay(200);
    assert.equal(stub.calls.length, 0);
    assert.equal(avatar.websocket, null);
  } finally {
    stub.restore();
    cleanupAvatar(avatar);
    await worker.close();
  }
});

test('does not reconnect after a fatal protocol error', async () => {
  const worker = new FakeWorker();
  await worker.listen();

  const avatar = makeAvatar();
  const stub = stubFetch(() => new Response('{}', { status: 500 }));

  try {
    const socket = await connectAndStart(avatar, worker);
    avatar.handleMessage(socket, JSON.stringify({
      type: 'error',
      code: 'INTERNAL_ERROR',
      message: 'fatal failure',
      fatal: true,
    }));
    assert.ok(avatar.protocolError instanceof FaceModeProtocolError);
    worker.terminateAll();
    await delay(200);
    assert.equal(stub.calls.length, 0);
  } finally {
    stub.restore();
    cleanupAvatar(avatar);
    await worker.close();
  }
});

test('does not reconnect after the worker ends the session', async () => {
  const worker = new FakeWorker();
  await worker.listen();

  const avatar = makeAvatar();
  const stub = stubFetch(() => new Response('{}', { status: 500 }));

  try {
    const socket = await connectAndStart(avatar, worker);
    avatar.handleMessage(socket, JSON.stringify({ type: 'ended' }));
    worker.terminateAll();
    await delay(200);
    assert.equal(stub.calls.length, 0);
    assert.equal(avatar.protocolError, null);
  } finally {
    stub.restore();
    cleanupAvatar(avatar);
    await worker.close();
  }
});

test('does not reconnect when the socket drops before protocol start', async () => {
  const worker = new FakeWorker({ autoStart: false });
  await worker.listen();

  const avatar = makeAvatar();
  const stub = stubFetch(() => new Response('{}', { status: 500 }));

  try {
    const socket = await avatar.connectWebSocket(worker.url, 'token-1');
    avatar.websocket = socket;
    avatar.bindWebSocketEvents(socket);
    avatar.protocolStarted = testDeferred();
    worker.terminateAll();
    await waitFor(() => avatar.protocolError !== null);
    await delay(100);
    assert.equal(stub.calls.length, 0);
    assert.match(avatar.protocolError.message, /closed unexpectedly/);
  } finally {
    stub.restore();
    cleanupAvatar(avatar);
    await worker.close();
  }
});

test('cleans up every listener and the keepalive timer on the dead socket', async () => {
  const worker = new FakeWorker();
  await worker.listen();

  const avatar = makeAvatar();
  const stub = stubFetch(() => new Response('{}', { status: 500 }));

  try {
    const socket = await connectAndStart(avatar, worker);
    assert.ok(avatar.keepaliveTimer);
    avatar.sessionTerminated = true;
    worker.terminateAll();
    await waitFor(() => avatar.websocket === null);
    assert.equal(socket.listenerCount('message'), 0);
    assert.equal(socket.listenerCount('close'), 0);
    assert.equal(socket.listenerCount('error'), 0);
    assert.equal(avatar.keepaliveTimer, null);
    await delay(100);
    assert.equal(stub.calls.length, 0);
  } finally {
    stub.restore();
    cleanupAvatar(avatar);
    await worker.close();
  }
});
