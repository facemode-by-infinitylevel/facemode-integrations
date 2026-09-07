import assert from 'node:assert/strict';
import test from 'node:test';

import { parseSessionDetails } from '../dist/models.js';

const room = {
  type: 'livekit',
  url: 'wss://livekit.example.test',
  token: 'viewer-token',
  name: 'room-123',
};

test('parses a pending canonical ingestion assignment without credentials', () => {
  const details = parseSessionDetails({
    sessionId: 'session-123',
    room,
    ingestion: {
      ready: false,
      authority: 'websocket',
    },
    workerStatus: 'ASSIGNING',
  });

  assert.equal(details.sessionId, 'session-123');
  assert.deepEqual(details.room, room);
  assert.deepEqual(details.ingestion, {
    ready: false,
    authority: 'websocket',
  });
  assert.equal(details.workerStatus, 'ASSIGNING');
});

test('preserves room credentials while parsing a ready polling response', () => {
  const initial = parseSessionDetails({
    sessionId: 'session-123',
    room,
    ingestion: { ready: false },
  });
  const details = parseSessionDetails(
    {
      sessionId: 'session-123',
      workerStatus: 'ASSIGNED',
      ingestion: {
        ready: true,
        authority: 'websocket',
        url: 'wss://worker.example.test/ws/session-123',
        wsToken: 'one-time-token',
        headers: {
          'X-Runpod-Worker-Id': 'strict worker-123',
          ignored: 17,
        },
      },
    },
    initial,
  );

  assert.deepEqual(details.room, room);
  assert.deepEqual(details.ingestion, {
    ready: true,
    authority: 'websocket',
    url: 'wss://worker.example.test/ws/session-123',
    wsToken: 'one-time-token',
    headers: { 'X-Runpod-Worker-Id': 'strict worker-123' },
  });
});

test('rejects a response that claims ingestion is ready without credentials', () => {
  assert.throws(
    () => parseSessionDetails({
      sessionId: 'session-123',
      room,
      ingestion: { ready: true },
    }),
    /ready ingestion without WebSocket credentials/,
  );
});
