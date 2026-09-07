export interface LiveKitRoom {
  readonly type: 'livekit';
  readonly url: string;
  readonly token: string;
  readonly name?: string;
}

export interface SessionRequest {
  readonly avatarId: string;
  readonly room: LiveKitRoom;
  readonly livekit_room_id: string;
  readonly waitForIngestion: boolean;
}

export interface IngestionDetails {
  readonly ready: boolean;
  readonly authority?: 'control' | 'websocket' | string;
  readonly url?: string;
  readonly wsToken?: string;
  readonly headers?: Readonly<Record<string, string>>;
}

export interface SessionDetails {
  readonly sessionId: string;
  readonly roomName: string;
  readonly room: LiveKitRoom;
  readonly ingestion: IngestionDetails;
  readonly workerStatus?: string;
  readonly avatarParticipantIdentity?: string;
}

export function parseSessionDetails(
  payload: Record<string, unknown>,
  fallback?: Pick<SessionDetails, 'room' | 'roomName'>,
): SessionDetails {
  const root = isRecord(payload.data) ? payload.data : payload;
  const session = isRecord(root.session) ? root.session : {};
  const ingestion = isRecord(root.ingestion)
    ? root.ingestion
    : isRecord(session.ingestion)
      ? session.ingestion
      : {};
  const sessionId = firstText(session.id, session.sessionId, root.sessionId, root.id, root.jobId);
  if (!sessionId) {
    throw new Error('FaceMode session response is missing a session ID');
  }

  const url = firstText(ingestion.url, ingestion.websocketUrl, root.websocketUrl, root.websocket_url);
  const wsToken = firstText(
    ingestion.wsToken,
    ingestion.ws_token,
    ingestion.token,
    root.ingestionToken,
    root.ingestion_token,
  );
  const ready = ingestion.ready === true || (!('ready' in ingestion) && Boolean(url && wsToken));
  if (ready && (!url || !wsToken)) {
    throw new Error('FaceMode reported ready ingestion without WebSocket credentials');
  }
  const headers = parseHeaders(ingestion.headers);

  return {
    sessionId,
    roomName: firstText(root.roomName, session.roomName, fallback?.roomName),
    room: parseRoom(root.room ?? session.room, root, session, fallback?.room),
    ingestion: {
      ready,
      ...(typeof ingestion.authority === 'string' ? { authority: ingestion.authority } : {}),
      ...(url ? { url } : {}),
      ...(wsToken ? { wsToken } : {}),
      ...(headers ? { headers } : {}),
    },
    ...(firstText(root.workerStatus, root.worker_status, session.workerStatus, session.worker_status)
      ? { workerStatus: firstText(root.workerStatus, root.worker_status, session.workerStatus, session.worker_status) }
      : {}),
    ...(firstText(
      root.avatarParticipantIdentity,
      root.avatar_participant_identity,
      session.avatarParticipantIdentity,
      session.avatar_participant_identity,
    )
      ? {
          avatarParticipantIdentity: firstText(
            root.avatarParticipantIdentity,
            root.avatar_participant_identity,
            session.avatarParticipantIdentity,
            session.avatar_participant_identity,
          ),
        }
      : {}),
  };
}

function parseRoom(
  value: unknown,
  root: Record<string, unknown>,
  session: Record<string, unknown>,
  fallback?: LiveKitRoom,
): LiveKitRoom {
  if (isRecord(value) && value.type === 'livekit') {
    const url = value.url;
    const token = value.token;
    if (typeof url === 'string' && url && typeof token === 'string' && token) {
      const name = typeof value.name === 'string' && value.name ? value.name : undefined;
      return { type: 'livekit', url, token, ...(name ? { name } : {}) };
    }
  }
  if (fallback) return fallback;
  throw new Error('FaceMode session response is missing a LiveKit room');
}

function parseHeaders(value: unknown): Readonly<Record<string, string>> | undefined {
  if (!isRecord(value)) return undefined;
  const headers: Record<string, string> = {};
  for (const [name, headerValue] of Object.entries(value)) {
    if (typeof headerValue === 'string' && name) headers[name] = headerValue;
  }
  return Object.keys(headers).length ? headers : undefined;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function firstText(...values: unknown[]): string {
  for (const value of values) {
    if (value !== undefined && value !== null && String(value)) return String(value);
  }
  return '';
}
