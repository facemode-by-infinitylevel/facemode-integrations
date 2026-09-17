import { log, voice } from '@livekit/agents';
import type { AudioFrame, Room } from '@livekit/rtc-node';
import WebSocket from 'ws';
import { FaceModeApiError, FaceModeProtocolError } from './exceptions.js';
import {
  parseSessionDetails,
  type LiveKitRoom,
  type SessionDetails,
  type SessionInputProvider,
  type SessionRequest,
} from './models.js';

export type StartOptions = {
  room: LiveKitRoom;
};

const MIN_INPUT_SAMPLE_RATE = 8000;
const MAX_INPUT_SAMPLE_RATE = 48000;
export const INGESTION_READY_TIMEOUT_MS = 240000;
export const WS_HANDSHAKE_TIMEOUT_MS = 240000;
const INGESTION_INITIAL_RETRY_MS = 250;
const INGESTION_MAX_RETRY_MS = 2000;
const APPLICATION_KEEPALIVE_MS = 15000;
const END_ACK_TIMEOUT_MS = 3000;
const PROTOCOL_RESPONSE_TIMEOUT_MS = 15000;
const RECONNECT_MAX_ATTEMPTS = 2;
const RECONNECT_BACKOFF_MS = 500;
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

type ReconnectAttempt = {
  socket: WebSocket;
  closed: ProtocolStarted;
  closeCode?: number;
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
  promise.catch(() => undefined);
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
  readonly inputProvider: SessionInputProvider | undefined;
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
  private sessionTerminated = false;
  private reconnectPromise: Promise<void> | null = null;
  private reconnectAttempt: ReconnectAttempt | null = null;

  constructor(options: {
    apiKey: string;
    avatarId?: string;
    apiUrl?: string;
    avatarParticipantIdentity?: string;
    avatarParticipantName?: string;
    inputProvider?: SessionInputProvider;
  }) {
    super();
    if (!options.apiKey) throw new Error('apiKey is required');
    this.apiKey = options.apiKey;
    this.avatarId = options.avatarId ?? '';
    this.apiUrl = (options.apiUrl ?? 'https://api.facemode.io/api').replace(/\/+$/, '');
    this.avatarParticipantIdentity = options.avatarParticipantIdentity ?? 'facemode-avatar';
    this.avatarParticipantName = options.avatarParticipantName ?? 'FaceMode Avatar';
    this.inputProvider = options.inputProvider;
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
      this.sessionTerminated = false;
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
    const pendingReconnect = this.reconnectPromise;
    if (pendingReconnect && !this.stopped && !this.sessionTerminated) {
      await pendingReconnect;
    }
    if (this.protocolError) throw this.protocolError;
    const socket = this.websocket;
    if (
      !socket
      || socket.readyState !== WebSocket.OPEN
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

    const pending = this.protocolStarted;
    if (pending.sent) {
      await pending.promise;
      if (this.protocolError) throw this.protocolError;
      return;
    }

    pending.sent = true;
    socket.send(this.buildStartMessage());
    await waitWithTimeout(pending.promise, PROTOCOL_RESPONSE_TIMEOUT_MS, 'FaceMode protocol negotiation timed out');
    if (this.protocolError) throw this.protocolError;
    if (!this.protocolStartedAck) {
      throw new FaceModeProtocolError('FaceMode protocol negotiation did not start');
    }
  }

  async sendAudioFrame(frame: AudioFrame): Promise<void> {
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
    await this.deliverPayload(buffer);
  }

  async sendControl(type: string, seq: number): Promise<void> {
    await this.deliverPayload(JSON.stringify({ type, seq }));
  }

  nextSequence(): number {
    return this.sequence++;
  }

  reconnect(): Promise<void> {
    if (this.reconnectPromise) return this.reconnectPromise;
    if (this.stopped || this.sessionTerminated || !this.session || !this.inputFormat) {
      return Promise.resolve();
    }
    const promise = (async () => {
      try {
        await this.performReconnect();
      } catch (error) {
        this.setProtocolError(error instanceof Error ? error : new Error(String(error)));
      } finally {
        this.reconnectPromise = null;
      }
    })();
    this.reconnectPromise = promise;
    return promise;
  }

  private async performReconnect(): Promise<void> {
    const session = this.session;
    const inputFormat = this.inputFormat;
    if (!session || !inputFormat) return;
    this.protocolStartedAck = false;
    let lastError: Error | null = null;
    for (let attempt = 1; attempt <= RECONNECT_MAX_ATTEMPTS; attempt += 1) {
      if (this.stopped || this.sessionTerminated || this.protocolError) return;
      if (attempt > 1) await sleep(RECONNECT_BACKOFF_MS);
      if (this.stopped || this.sessionTerminated || this.protocolError) return;
      let socket: WebSocket;
      try {
        const fresh = await this.requestReconnectCredentials(session);
        const url = fresh.ingestion.url;
        const wsToken = fresh.ingestion.wsToken;
        if (!fresh.ingestion.ready || !url || !wsToken) {
          throw new FaceModeApiError('FaceMode reconnect did not return usable WebSocket credentials');
        }
        if (fresh.workerStatus === 'FAILED' || fresh.workerStatus === 'ENDED') {
          throw new FaceModeApiError(`FaceMode session entered ${fresh.workerStatus.toLowerCase()} state`);
        }
        socket = await this.connectWebSocket(url, wsToken, fresh.ingestion.headers);
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        continue;
      }
      if (this.stopped || this.sessionTerminated || this.protocolError) {
        this.teardownSocket(socket);
        return;
      }
      this.websocket = socket;
      const attemptState: ReconnectAttempt = { socket, closed: deferred() };
      this.reconnectAttempt = attemptState;
      const started = deferred();
      this.protocolStarted = started;
      started.sent = true;
      this.protocolStartedAck = false;
      this.bindWebSocketEvents(socket);
      try {
        socket.send(this.buildStartMessage());
      } catch (error) {
        lastError = error instanceof Error ? error : new Error(String(error));
        this.reconnectAttempt = null;
        this.teardownSocket(socket);
        if (this.websocket === socket) this.websocket = null;
        continue;
      }
      let timer: NodeJS.Timeout | undefined;
      const outcome = await Promise.race([
        started.promise.then(
          (): 'started' => 'started',
          (error: unknown): Error => (error instanceof Error ? error : new Error(String(error))),
        ),
        attemptState.closed.promise.then((): 'closed' => 'closed'),
        new Promise<'timeout'>((resolve) => {
          timer = setTimeout(() => resolve('timeout'), PROTOCOL_RESPONSE_TIMEOUT_MS);
        }),
      ]).finally(() => {
        if (timer) clearTimeout(timer);
      });
      if (this.reconnectAttempt === attemptState) this.reconnectAttempt = null;
      if (outcome === 'started' && socket.readyState === WebSocket.OPEN && this.websocket === socket) {
        this.startApplicationKeepalive();
        log().info({ session: this.sessionLogMarker }, 'FaceMode WebSocket reconnected');
        return;
      }
      this.teardownSocket(socket);
      if (this.websocket === socket) this.websocket = null;
      this.protocolStartedAck = false;
      if (this.protocolError) return;
      lastError = outcome instanceof Error
        ? outcome
        : new FaceModeProtocolError(
            outcome === 'timeout'
              ? 'FaceMode reconnect timed out waiting for protocol start'
              : `FaceMode reconnect socket closed before protocol start${attemptState.closeCode !== undefined ? ` (code=${attemptState.closeCode})` : ''}`,
          );
    }
    this.setProtocolError(new FaceModeProtocolError(
      `FaceMode reconnect failed after ${RECONNECT_MAX_ATTEMPTS} attempts${lastError ? ` (${safeErrorType(lastError)})` : ''}`,
    ));
  }

  private async requestReconnectCredentials(session: SessionDetails): Promise<SessionDetails> {
    const response = await fetch(`${this.apiUrl}/sessions/${encodeURIComponent(session.sessionId)}/reconnect`, {
      method: 'POST',
      headers: this.apiHeaders(),
      body: JSON.stringify({ room: session.room }),
    });
    const payload = await parseApiJson(response, 'FaceMode session reconnect');
    return parseSessionDetails(payload, {
      sessionId: session.sessionId,
      room: session.room,
      roomName: session.roomName,
    });
  }

  private buildStartMessage(): string {
    const session = this.session;
    const inputFormat = this.inputFormat;
    if (!session || !inputFormat) {
      throw new FaceModeProtocolError('FaceMode session is not ready to start');
    }
    return JSON.stringify({
      type: 'start',
      session_id: session.sessionId,
      audio_encoding: 'pcm_s16le',
      sample_rate: inputFormat.sampleRate,
      channels: inputFormat.channels,
      avatar_id: this.avatarId,
      metadata: { source: 'livekit-agents-js' },
    });
  }

  private async writableSocket(): Promise<WebSocket | null> {
    // Yield one microtask so a reconnect() triggered synchronously after the
    // caller invoked send* is observed before the socket is chosen.
    await Promise.resolve();
    const pending = this.reconnectPromise;
    if (pending && !this.stopped && !this.sessionTerminated) {
      await pending;
    }
    if (this.protocolError) throw this.protocolError;
    const current = this.websocket;
    if (current && current.readyState === WebSocket.OPEN) return current;
    if (this.stopped || this.sessionTerminated || !this.session || !this.inputFormat) {
      return null;
    }
    await this.reconnect();
    if (this.protocolError) throw this.protocolError;
    const next = this.websocket;
    return next && next.readyState === WebSocket.OPEN ? next : null;
  }

  private async deliverPayload(payload: string | Buffer): Promise<void> {
    for (;;) {
      const socket = await this.writableSocket();
      if (!socket) {
        if (this.stopped || this.sessionTerminated) return;
        throw this.protocolError ?? new FaceModeProtocolError('FaceMode WebSocket is not connected');
      }
      try {
        socket.send(payload);
      } catch (error) {
        if (this.stopped) return;
        if (socket.readyState === WebSocket.OPEN) {
          throw error instanceof Error ? error : new FaceModeProtocolError('FaceMode WebSocket send failed');
        }
        await this.reconnect();
        continue;
      }
      // Resolve only after the write has had at least one full event-loop
      // turn to reach the peer; send() returning does not mean delivery.
      await nextCheckPhase();
      await nextCheckPhase();
      return;
    }
  }

  private teardownSocket(socket: WebSocket): void {
    try {
      socket.removeAllListeners();
    } catch {
      // defensive cleanup only
    }
    try {
      socket.close();
    } catch {
      // the transport is already gone
    }
    try {
      socket.terminate();
    } catch {
      // not every socket test double implements terminate
    }
  }

  private async createSession(room: LiveKitRoom, roomName: string): Promise<SessionDetails> {
    const request: SessionRequest = {
      avatarId: this.avatarId,
      room,
      livekit_room_id: roomName,
      ...(this.inputProvider ? { inputProvider: this.inputProvider } : {}),
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
      if (session.workerStatus === 'FAILED' || session.workerStatus === 'ENDED') {
        throw new FaceModeApiError(`FaceMode ingestion worker entered ${session.workerStatus.toLowerCase()} state`);
      }
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
      const socket = new WebSocket(url, [`facemode.${token}`], {
        handshakeTimeout: WS_HANDSHAKE_TIMEOUT_MS,
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
    socket.on('message', (data) => this.handleMessage(socket, data.toString()));
    socket.on('error', (error) => this.handleSocketError(socket, error));
    socket.on('close', (code) => this.handleSocketClosed(socket, code));
  }

  private handleSocketError(socket: WebSocket, error: unknown): void {
    const attempt = this.reconnectAttempt;
    if (attempt && attempt.socket === socket) {
      attempt.closed.resolve();
      return;
    }
    if (socket !== this.websocket) return;
    if (this.stopped || this.sessionTerminated) return;
    const protocolError = error instanceof Error ? error : new Error(String(error));
    if (!this.protocolStartedAck) {
      if (!this.reconnectPromise) this.setProtocolError(protocolError);
      return;
    }
    log().warn(
      { session: this.sessionLogMarker, errorType: safeErrorType(protocolError) },
      'FaceMode WebSocket reported an error',
    );
  }

  private handleSocketClosed(socket: WebSocket, code: number): void {
    this.teardownSocket(socket);
    if (this.websocket === socket) this.websocket = null;
    const attempt = this.reconnectAttempt;
    if (attempt && attempt.socket === socket) {
      this.reconnectAttempt = null;
      attempt.closeCode = code;
      attempt.closed.resolve();
      return;
    }
    this.stopApplicationKeepalive();
    if (this.stopped || this.sessionTerminated) return;
    if (this.protocolError) return;
    if (!this.protocolStartedAck) {
      if (!this.reconnectPromise) {
        this.setProtocolError(new FaceModeProtocolError(`FaceMode WebSocket closed unexpectedly (code=${code})`));
      }
      return;
    }
    log().warn(
      { session: this.sessionLogMarker, code },
      'FaceMode WebSocket closed unexpectedly',
    );
    void this.reconnect();
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
    const terminal = this.protocolError ?? error;
    this.protocolError = terminal;
    const started = this.protocolStarted;
    if (started) {
      started.promise.catch(() => undefined);
      started.reject(terminal);
    }
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
    this.sessionTerminated = false;
  }

  private handleMessage(socket: WebSocket, raw: string): void {
    if (socket !== this.websocket && socket !== this.reconnectAttempt?.socket) return;
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
      this.sessionTerminated = true;
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

function nextCheckPhase(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
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
