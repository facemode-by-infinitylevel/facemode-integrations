import { log, voice } from '@livekit/agents';
import type { AudioFrame, Room } from '@livekit/rtc-node';
import WebSocket from 'ws';
import { FaceModeApiError, FaceModeProtocolError } from './exceptions.js';
import {
  parseSessionDetails,
  type LiveKitRoom,
  type SessionDetails,
  type SessionRequest,
} from './models.js';

export type StartOptions = {
  room: LiveKitRoom;
};

const MIN_INPUT_SAMPLE_RATE = 8000;
const MAX_INPUT_SAMPLE_RATE = 48000;
const INGESTION_READY_TIMEOUT_MS = 60000;
const INGESTION_INITIAL_RETRY_MS = 250;
const INGESTION_MAX_RETRY_MS = 2000;
const APPLICATION_KEEPALIVE_MS = 15000;
const END_ACK_TIMEOUT_MS = 3000;
const PREFERRED_SAMPLE_RATES = new Set([
  8000, 11025, 12000, 16000, 22050, 24000, 32000, 44100, 48000,
]);

function safeErrorType(error: unknown): string {
  return error instanceof Error && ['AbortError', 'TimeoutError', 'TypeError'].includes(error.name)
    ? error.name
    : 'Error';
}

type ProtocolStarted = {
  resolve: () => void;
  reject: (error: Error) => void;
  promise: Promise<void>;
  sent?: boolean;
};

type InputFormat = {
  sampleRate: number;
  channels: number;
};

type AudioOutputSlot = {
  audio: voice.AudioOutput | null;
};

function deferred(): ProtocolStarted {
  let resolvePromise!: () => void;
  let rejectPromise!: (error: Error) => void;
  const promise = new Promise<void>((resolve, reject) => {
    resolvePromise = resolve;
    rejectPromise = reject;
  });
  return { resolve: resolvePromise, reject: rejectPromise, promise };
}

class FaceModeAudioOutput extends voice.AudioOutput {
  private readonly owner: AvatarSession;
  private utteranceStarted = false;

  constructor(owner: AvatarSession) {
    super(undefined, undefined, { pause: false });
    this.owner = owner;
  }

  override async captureFrame(frame: AudioFrame): Promise<void> {
    await super.captureFrame(frame);
    if (!this.utteranceStarted) {
      await this.owner.ensureProtocolStarted(frame.sampleRate, frame.channels);
      await this.owner.sendControl('start_utterance', this.owner.nextSequence());
      this.utteranceStarted = true;
      this.onPlaybackStarted(Date.now());
    }
    await this.owner.sendAudioFrame(frame);
  }

  override flush(): void {
    super.flush();
    if (!this.utteranceStarted) return;
    this.utteranceStarted = false;
    void this.owner.sendControl('end_utterance', this.owner.nextSequence());
    this.onPlaybackFinished({ playbackPosition: 0, interrupted: false });
  }

  override clearBuffer(): void {
    if (!this.utteranceStarted) return;
    this.utteranceStarted = false;
    void this.owner.sendControl('cancel_utterance', this.owner.nextSequence());
    this.onPlaybackFinished({ playbackPosition: 0, interrupted: true });
  }
}

export class AvatarSession extends voice.AvatarSession {
  readonly avatarId: string;
  readonly apiKey: string;
  readonly apiUrl: string;
  avatarParticipantIdentity: string;
  readonly avatarParticipantName: string;

  private room: Room | null = null;
  private session: SessionDetails | null = null;
  private websocket: WebSocket | null = null;
  private protocolStarted: ProtocolStarted | null = null;
  private sessionEnded: ProtocolStarted | null = null;
  private protocolStartedAck = false;
  private protocolError: Error | null = null;
  private avatarVideoPromise: Promise<void> | null = null;
  private avatarVideoResolve: (() => void) | null = null;
  private keepaliveTimer: NodeJS.Timeout | null = null;
  private inputFormat: InputFormat | null = null;
  private outputSlot: AudioOutputSlot | null = null;
  private sessionLogMarker: string | null = null;
  private sequence = 0;
  private stopped = false;

  constructor(options: {
    apiKey: string;
    avatarId?: string;
    apiUrl?: string;
    avatarParticipantIdentity?: string;
    avatarParticipantName?: string;
  }) {
    super();
    if (!options.apiKey) throw new Error('apiKey is required');
    this.apiKey = options.apiKey;
    this.avatarId = options.avatarId ?? '';
    this.apiUrl = (options.apiUrl ?? 'https://api.facemode.io/api').replace(/\/+$/, '');
    this.avatarParticipantIdentity = options.avatarParticipantIdentity ?? 'facemode-avatar';
    this.avatarParticipantName = options.avatarParticipantName ?? 'FaceMode Avatar';
  }

  get avatarIdentity(): string {
    return this.avatarParticipantIdentity;
  }

  get provider(): string {
    return 'facemode';
  }

  get sessionId(): string | null {
    return this.session?.sessionId ?? null;
  }

  async start(agentSession: voice.AgentSession, room: Room, options?: StartOptions): Promise<void> {
    const roomConfig = normalizeLiveKitRoom(options?.room);
    const roomName = roomConfig.name || room.name || tokenRoomHint(roomConfig.token);
    if (!roomName) throw new FaceModeApiError('the exact LiveKit room name is required');
    if (this.avatarParticipantIdentity === 'facemode-avatar') {
      const tokenIdentity = tokenIdentityHint(roomConfig.token);
      if (tokenIdentity) this.avatarParticipantIdentity = tokenIdentity;
    }

    let baseStarted = false;
    try {
      await super.start(agentSession, room);
      baseStarted = true;
      this.room = room;
      this.stopped = false;
      this.sequence = 0;
      this.protocolStartedAck = false;
      this.protocolError = null;
      this.inputFormat = null;
      this.session = await this.createSession(roomConfig, roomName);
      this.session = await this.waitForIngestion(this.session);
      this.sessionLogMarker = sanitizeSessionMarker(this.session.sessionId);
      this.protocolStarted = deferred();
      this.sessionEnded = deferred();
      this.websocket = await this.connectWebSocket(
        this.session.ingestion.url!,
        this.session.ingestion.wsToken!,
        this.session.ingestion.headers,
      );
      this.bindWebSocketEvents(this.websocket);
      this.startApplicationKeepalive();
      await this.negotiateConfiguredTts(agentSession);

      const output = (agentSession as unknown as { output?: AudioOutputSlot }).output;
      if (!output) {
        throw new FaceModeProtocolError('LiveKit AgentSession does not expose an output');
      }
      this.outputSlot = output;
      output.audio = new FaceModeAudioOutput(this);
      this.avatarVideoPromise = new Promise<void>((resolve) => {
        this.avatarVideoResolve = resolve;
      });
      this.bindRoomVideoEvents(room);
      if (this.hasRemoteVideoTrack(room)) this.avatarVideoResolve?.();
      log().info({ session: this.sessionLogMarker }, 'FaceMode avatar session started');
    } catch (error) {
      await this.rollbackStart(baseStarted);
      throw error;
    }
  }

  async waitForJoin({ timeout = 30000 }: { timeout?: number | null } = {}): Promise<void> {
    if (!this.avatarVideoPromise) return;
    if (timeout === null) {
      await this.avatarVideoPromise;
      return;
    }
    let timer: NodeJS.Timeout | undefined;
    try {
      await Promise.race([
        this.avatarVideoPromise,
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new FaceModeProtocolError('Timed out waiting for FaceMode avatar video track')), timeout);
        }),
      ]);
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  async stop(): Promise<void> {
    if (this.stopped) return;
    this.stopped = true;
    this.stopApplicationKeepalive();
    const socket = this.websocket;
    const ended = this.sessionEnded?.promise;
    if (socket?.readyState === WebSocket.OPEN && this.protocolStartedAck && this.session) {
      try {
        await this.sendControl('end_session', this.nextSequence());
        if (ended) await waitWithTimeout(ended, END_ACK_TIMEOUT_MS, 'Timed out waiting for FaceMode session end acknowledgement');
      } catch (error) {
        log().warn(
          { session: this.sessionLogMarker, errorType: safeErrorType(error) },
          'FaceMode session shutdown acknowledgement was not received',
        );
      }
    }
    if (socket) {
      socket.close();
      this.websocket = null;
    }
    this.resetRuntimeState();
    await super.aclose();
  }

  async aclose(): Promise<void> {
    await this.stop();
  }

  async ensureProtocolStarted(sampleRate: number, channels: number): Promise<void> {
    if (this.protocolError) throw this.protocolError;
    if (
      !this.websocket
      || this.websocket.readyState !== WebSocket.OPEN
      || !this.session
      || !this.protocolStarted
    ) {
      throw new FaceModeProtocolError('FaceMode WebSocket is not connected');
    }

    const inputFormat = validateInputFormat(sampleRate, channels);
    if (
      this.inputFormat
      && (this.inputFormat.sampleRate !== inputFormat.sampleRate || this.inputFormat.channels !== inputFormat.channels)
    ) {
      throw new FaceModeProtocolError(
        `LiveKit TTS audio format changed after negotiation: expected ${this.inputFormat.sampleRate}Hz/${this.inputFormat.channels}ch, received ${inputFormat.sampleRate}Hz/${inputFormat.channels}ch`,
      );
    }
    this.inputFormat ??= inputFormat;

    if (this.protocolStarted.sent) {
      await this.protocolStarted.promise;
      if (this.protocolError) throw this.protocolError;
      return;
    }

    this.protocolStarted.sent = true;
    this.websocket.send(JSON.stringify({
      type: 'start',
      session_id: this.session.sessionId,
      audio_encoding: 'pcm_s16le',
      sample_rate: inputFormat.sampleRate,
      channels: inputFormat.channels,
      avatar_id: this.avatarId,
      metadata: { source: 'livekit-agents-js' },
    }));
    await waitWithTimeout(this.protocolStarted.promise, 15000, 'FaceMode protocol negotiation timed out');
    if (this.protocolError) throw this.protocolError;
    if (!this.protocolStartedAck) {
      throw new FaceModeProtocolError('FaceMode protocol negotiation did not start');
    }
  }

  async sendAudioFrame(frame: AudioFrame): Promise<void> {
    if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) {
      throw this.protocolError ?? new FaceModeProtocolError('FaceMode WebSocket is not connected');
    }
    if (!this.inputFormat) {
      throw new FaceModeProtocolError('FaceMode audio was sent before protocol negotiation');
    }
    if (
      frame.sampleRate !== this.inputFormat.sampleRate
      || frame.channels !== this.inputFormat.channels
    ) {
      throw new FaceModeProtocolError(
        `LiveKit TTS audio format changed after negotiation: expected ${this.inputFormat.sampleRate}Hz/${this.inputFormat.channels}ch, received ${frame.sampleRate}Hz/${frame.channels}ch`,
      );
    }
    const data = frame.data;
    const buffer = Buffer.from(data.buffer, data.byteOffset, data.byteLength);
    const bytesPerSampleFrame = Int16Array.BYTES_PER_ELEMENT * this.inputFormat.channels;
    if (buffer.byteLength % bytesPerSampleFrame !== 0) {
      throw new FaceModeProtocolError(
        `LiveKit PCM frame is not aligned to ${this.inputFormat.channels} channel 16-bit samples`,
      );
    }
    this.websocket.send(buffer);
  }

  async sendControl(type: string, seq: number): Promise<void> {
    if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) {
      if (this.stopped) return;
      throw this.protocolError ?? new FaceModeProtocolError('FaceMode WebSocket is not connected');
    }
    this.websocket.send(JSON.stringify({ type, seq }));
  }

  nextSequence(): number {
    return this.sequence++;
  }

  private async createSession(room: LiveKitRoom, roomName: string): Promise<SessionDetails> {
    const request: SessionRequest = {
      avatarId: this.avatarId,
      room,
      livekit_room_id: roomName,
      waitForIngestion: true,
    };
    const response = await fetch(`${this.apiUrl}/sessions`, {
      method: 'POST',
      headers: this.apiHeaders(),
      body: JSON.stringify(request),
    });
    const payload = await parseApiJson(response, 'FaceMode session creation');
    return parseSessionDetails(payload);
  }

  private async waitForIngestion(initialSession: SessionDetails): Promise<SessionDetails> {
    let session = initialSession;
    let delay = INGESTION_INITIAL_RETRY_MS;
    const deadline = Date.now() + INGESTION_READY_TIMEOUT_MS;
    while (!session.ingestion.ready) {
      if (Date.now() >= deadline) {
        throw new FaceModeApiError('Timed out waiting for FaceMode ingestion assignment');
      }
      await sleep(delay);
      delay = Math.min(Math.ceil(delay * 1.5), INGESTION_MAX_RETRY_MS);
      const response = await fetch(`${this.apiUrl}/sessions/${encodeURIComponent(session.sessionId)}`, {
        headers: this.apiHeaders(false),
      });
      const payload = await parseApiJson(response, 'FaceMode ingestion status');
      session = parseSessionDetails(payload, session);
      if (session.workerStatus === 'FAILED' || session.workerStatus === 'ENDED') {
        throw new FaceModeApiError(`FaceMode ingestion worker entered ${session.workerStatus.toLowerCase()} state`);
      }
    }
    if (!session.ingestion.url || !session.ingestion.wsToken) {
      throw new FaceModeApiError('FaceMode ingestion assignment is missing WebSocket credentials');
    }
    return session;
  }

  private apiHeaders(withJson = true): Record<string, string> {
    return {
      Authorization: `Bearer ${this.apiKey}`,
      ...(withJson ? { 'Content-Type': 'application/json' } : {}),
    };
  }

  private connectWebSocket(
    url: string,
    token: string,
    headers?: Readonly<Record<string, string>>,
  ): Promise<WebSocket> {
    return new Promise((resolve, reject) => {
      const socket = new WebSocket(url, [`aivatar.${token}`], {
        handshakeTimeout: 60000,
        perMessageDeflate: false,
        ...(headers ? { headers: { ...headers } } : {}),
      });
      const onOpen = (): void => {
        socket.off('error', onError);
        socket.off('close', onClose);
        resolve(socket);
      };
      const onError = (error: Error): void => {
        socket.off('open', onOpen);
        socket.off('close', onClose);
        reject(error);
      };
      const onClose = (): void => {
        socket.off('open', onOpen);
        socket.off('error', onError);
        reject(new FaceModeProtocolError('FaceMode WebSocket closed before connecting'));
      };
      socket.once('open', onOpen);
      socket.once('error', onError);
      socket.once('close', onClose);
    });
  }

  private bindWebSocketEvents(socket: WebSocket): void {
    socket.on('message', (data) => this.handleMessage(data.toString()));
    socket.on('error', (error) => {
      const protocolError = error instanceof Error ? error : new Error(String(error));
      this.setProtocolError(protocolError);
    });
    socket.on('close', (code) => {
      this.stopApplicationKeepalive();
      if (this.stopped) return;
      const protocolError = new FaceModeProtocolError(
        `FaceMode WebSocket closed unexpectedly (code=${code})`,
      );
      this.setProtocolError(protocolError);
      log().warn(
        { session: this.sessionLogMarker, code },
        'FaceMode WebSocket closed unexpectedly',
      );
    });
  }

  private async negotiateConfiguredTts(agentSession: voice.AgentSession): Promise<void> {
    const tts = (agentSession as unknown as {
      tts?: { sampleRate?: unknown; numChannels?: unknown };
    }).tts;
    if (typeof tts?.sampleRate !== 'number' || typeof tts.numChannels !== 'number') return;
    await this.ensureProtocolStarted(tts.sampleRate, tts.numChannels);
  }

  private startApplicationKeepalive(): void {
    this.stopApplicationKeepalive();
    this.keepaliveTimer = setInterval(() => {
      if (!this.websocket || this.websocket.readyState !== WebSocket.OPEN) return;
      try {
        this.websocket.send(JSON.stringify({ type: 'ping', ts: Date.now() }));
      } catch (error) {
        this.setProtocolError(error instanceof Error ? error : new Error(String(error)));
      }
    }, APPLICATION_KEEPALIVE_MS);
  }

  private stopApplicationKeepalive(): void {
    if (!this.keepaliveTimer) return;
    clearInterval(this.keepaliveTimer);
    this.keepaliveTimer = null;
  }

  private setProtocolError(error: Error): void {
    if (!this.protocolError) this.protocolError = error;
    this.protocolStarted?.reject(error);
  }

  private async rollbackStart(baseStarted: boolean): Promise<void> {
    const sessionId = this.session?.sessionId;
    this.stopped = true;
    this.stopApplicationKeepalive();
    if (this.websocket) {
      this.websocket.close();
      this.websocket = null;
    }
    if (sessionId) {
      await this.requestSessionDeletion(sessionId);
    }
    this.resetRuntimeState();
    if (baseStarted) await super.aclose();
  }

  private async requestSessionDeletion(sessionId: string): Promise<void> {
    try {
      const response = await fetch(`${this.apiUrl}/sessions/${encodeURIComponent(sessionId)}`, {
        method: 'DELETE',
        headers: this.apiHeaders(false),
      });
      if (!response.ok && response.status !== 404) {
        log().warn({ session: sanitizeSessionMarker(sessionId), status: response.status }, 'FaceMode startup rollback cleanup was not accepted');
      }
    } catch (error) {
      log().warn(
        { session: sanitizeSessionMarker(sessionId), errorType: safeErrorType(error) },
        'FaceMode startup rollback cleanup request failed',
      );
    }
  }

  private resetRuntimeState(): void {
    this.stopApplicationKeepalive();
    if (this.outputSlot) this.outputSlot.audio = null;
    this.outputSlot = null;
    this.room = null;
    this.session = null;
    this.protocolStarted = null;
    this.sessionEnded = null;
    this.protocolStartedAck = false;
    this.protocolError = null;
    this.avatarVideoPromise = null;
    this.avatarVideoResolve = null;
    this.inputFormat = null;
    this.sessionLogMarker = null;
  }

  private handleMessage(raw: string): void {
    let message: Record<string, unknown>;
    try {
      message = JSON.parse(raw) as Record<string, unknown>;
    } catch {
      return;
    }
    if (message.type === 'started') {
      let error: FaceModeProtocolError | null = null;
      if (!this.session || String(message.session_id ?? '') !== this.session.sessionId) {
        error = new FaceModeProtocolError('FaceMode started response session ID did not match');
      } else if (message.server_sample_rate !== 48000 || message.server_channels !== 1) {
        error = new FaceModeProtocolError('FaceMode server reported an unsupported canonical audio format');
      }
      if (error) {
        this.setProtocolError(error);
        return;
      }
      this.protocolStartedAck = true;
      this.protocolStarted?.resolve();
      log().info({ session: this.sessionLogMarker }, 'FaceMode protocol started');
    } else if (message.type === 'error') {
      const error = new FaceModeProtocolError(`${String(message.code ?? 'INTERNAL_ERROR')}: ${String(message.message ?? 'FaceMode error')}`);
      if (message.fatal || !this.protocolStartedAck) this.setProtocolError(error);
    } else if (message.type === 'session_ending') {
      this.setProtocolError(new FaceModeProtocolError(`FaceMode session is ending: ${String(message.reason ?? 'unknown')}`));
    } else if (message.type === 'ended') {
      this.sessionEnded?.resolve();
    } else if (message.type === 'audio_ready') {
      this.avatarVideoResolve?.();
    }
  }

  private bindRoomVideoEvents(room: Room): void {
    const eventRoom = room as unknown as {
      on?: (event: string, callback: (...args: any[]) => void) => void;
    };
    eventRoom.on?.('trackSubscribed', (track: { kind?: string }, _publication: unknown, participant: { identity?: string }) => {
      if (track.kind === 'video' && participant.identity !== (room as any).localParticipant?.identity) {
        this.avatarVideoResolve?.();
      }
    });
  }

  private hasRemoteVideoTrack(room: Room): boolean {
    const remoteParticipants = (room as any).remoteParticipants;
    if (!remoteParticipants) return false;
    const participants = remoteParticipants instanceof Map ? [...remoteParticipants.values()] : Object.values(remoteParticipants);
    return participants.some((participant: any) => {
      const publications = participant.trackPublications instanceof Map
        ? [...participant.trackPublications.values()]
        : Object.values(participant.trackPublications ?? {});
      return publications.some((publication: any) => publication.track && publication.track.kind === 'video');
    });
  }
}

function validateInputFormat(sampleRate: number, channels: number): InputFormat {
  if (!Number.isInteger(sampleRate)) {
    throw new FaceModeProtocolError('LiveKit TTS sample rate must be an integer');
  }
  if (sampleRate < MIN_INPUT_SAMPLE_RATE || sampleRate > MAX_INPUT_SAMPLE_RATE) {
    throw new FaceModeProtocolError(
      `LiveKit TTS sample rate must be between ${MIN_INPUT_SAMPLE_RATE} and ${MAX_INPUT_SAMPLE_RATE}: ${sampleRate}`,
    );
  }
  if (!PREFERRED_SAMPLE_RATES.has(sampleRate)) {
    log().info({ sampleRate }, 'using uncommon LiveKit TTS sample rate');
  }
  if (![1, 2].includes(channels)) {
    throw new FaceModeProtocolError(`Unsupported LiveKit TTS channel count: ${channels}`);
  }
  return { sampleRate, channels };
}

async function parseApiJson(response: Response, operation: string): Promise<Record<string, unknown>> {
  const body = await response.text();
  if (!response.ok) {
    throw new FaceModeApiError(`${operation} failed (${response.status})`);
  }
  try {
    const payload = JSON.parse(body) as unknown;
    if (!payload || typeof payload !== 'object') throw new Error('not an object');
    return payload as Record<string, unknown>;
  } catch {
    throw new FaceModeApiError(`${operation} response was not JSON`);
  }
}

function waitWithTimeout(promise: Promise<void>, timeout: number, message: string): Promise<void> {
  let timer: NodeJS.Timeout | undefined;
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new FaceModeProtocolError(message)), timeout);
    }),
  ]).finally(() => {
    if (timer) clearTimeout(timer);
  });
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

function sanitizeSessionMarker(sessionId: string): string {
  return sessionId.replace(/[^A-Za-z0-9_-]/g, '_').slice(0, 96);
}

function normalizeLiveKitRoom(value: LiveKitRoom | undefined): LiveKitRoom {
  if (!value || value.type !== 'livekit') {
    throw new Error("room.type must be 'livekit'");
  }
  if (typeof value.url !== 'string' || !value.url) {
    throw new Error('room.url is required');
  }
  if (typeof value.token !== 'string' || !value.token) {
    throw new Error('room.token is required');
  }
  const name = typeof value.name === 'string' && value.name ? value.name : undefined;
  return {
    type: 'livekit',
    url: value.url,
    token: value.token,
    ...(name ? { name } : {}),
  };
}

function tokenIdentityHint(token: string): string | undefined {
  const payload = tokenClaimsHint(token);
  return typeof payload?.sub === 'string' && payload.sub ? payload.sub : undefined;
}

function tokenRoomHint(token: string): string | undefined {
  const payload = tokenClaimsHint(token);
  const video = payload?.video;
  if (!video || typeof video !== 'object') return undefined;
  const room = (video as Record<string, unknown>).room;
  return typeof room === 'string' && room ? room : undefined;
}

function tokenClaimsHint(token: string): Record<string, unknown> | undefined {
  try {
    const part = token.split('.')[1];
    const payload = JSON.parse(Buffer.from(part, 'base64url').toString('utf8')) as unknown;
    return payload && typeof payload === 'object' ? payload as Record<string, unknown> : undefined;
  } catch {
    return undefined;
  }
}
