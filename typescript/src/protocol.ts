/**
 * Wire shapes of the FEP-3ab2 control API, shared by the client and the server.
 *
 * The server constructs these bodies; the client parses them from untrusted
 * JSON. Keeping both sides on one definition keeps the two from drifting: a
 * renamed field is a compile error on the side that forgot.
 *
 * Error responses are RFC 9457 problem details: the `type`
 * member identifies the problem, `title` and `detail` are human-readable, and
 * `status` repeats the HTTP status code.
 */

export const PROBLEM_MEDIA_TYPE = 'application/problem+json';

/** Problem type URIs: FEP-3ab2's own, plus the two it reuses from FEP-c180. */
export const PROBLEM_TYPES = {
  /** 400: a topic does not conform to the topic structure. */
  invalidTopic: 'https://w3id.org/fep/3ab2#invalid-topic',
  /** 401: missing, expired, already used, or another Principal's ticket. */
  invalidTicket: 'https://w3id.org/fep/3ab2#invalid-ticket',
  /** 403: the request `Origin` is not allowlisted. */
  originNotAllowed: 'https://w3id.org/fep/3ab2#origin-not-allowed',
  /** 403: the Principal may not observe the requested topic. */
  principalNotAuthorized: 'https://w3id.org/fep/c180#principal-not-authorized',
  /** 429: ticket issuance or stream connections exceed the server's limit. */
  rateLimitExceeded: 'https://w3id.org/fep/c180#rate-limit-exceeded',
  /** Anything else: no semantics beyond the status code (RFC 9457 section 4.2.1). */
  aboutBlank: 'about:blank',
} as const;

export type ProblemType = (typeof PROBLEM_TYPES)[keyof typeof PROBLEM_TYPES];

export interface ProblemDetails {
  readonly type: string;
  readonly title: string;
  /** The HTTP status code, repeated in the body */
  readonly status: number;
  readonly detail: string;
}

const PROBLEM_TITLES: Readonly<Record<string, string>> = {
  [PROBLEM_TYPES.invalidTopic]: 'Invalid topic',
  [PROBLEM_TYPES.invalidTicket]: 'Invalid ticket',
  [PROBLEM_TYPES.originNotAllowed]: 'Origin not allowed',
  [PROBLEM_TYPES.principalNotAuthorized]: 'Principal not authorized',
  [PROBLEM_TYPES.rateLimitExceeded]: 'Rate limit exceeded',
};

const STATUS_PHRASES: Readonly<Record<number, string>> = {
  400: 'Bad Request',
  401: 'Unauthorized',
  403: 'Forbidden',
  404: 'Not Found',
  405: 'Method Not Allowed',
  406: 'Not Acceptable',
  409: 'Conflict',
  410: 'Gone',
  413: 'Content Too Large',
  415: 'Unsupported Media Type',
  422: 'Unprocessable Content',
  429: 'Too Many Requests',
  500: 'Internal Server Error',
  501: 'Not Implemented',
  503: 'Service Unavailable',
};

export function httpStatusPhrase(status: number): string {
  return STATUS_PHRASES[status] ?? `Status ${status}`;
}

/**
 * Builds a problem detail body.
 *
 * `title` is advisory prose about the type (RFC 9457 section 3.1.3). It defaults to the
 * type's own title, or to the HTTP status phrase for `about:blank`, which
 * section 4.2.1 requires.
 */
export function problem(
  type: string,
  status: number,
  detail: string,
  title?: string,
): ProblemDetails {
  return {
    type,
    title: title ?? PROBLEM_TITLES[type] ?? httpStatusPhrase(status),
    status,
    detail,
  };
}

export interface CreateSessionBody {
  /** Absolute URL. */
  readonly subscriptions_url: string;
  /** Absolute URL. */
  readonly stream_url: string;
  /** RFC 3339 UTC timestamp of ticket expiry, second precision. */
  readonly expires_at: string;
  /** Maximum seconds the server will go without sending a frame */
  readonly heartbeat_interval?: number;
  /** Whether topic wildcards are supported; defaults to `true` per the spec. */
  readonly wildcard_support: boolean;
}

export interface TopicsBody {
  readonly topics: string[];
}

export class ProtocolError extends Error {
  override readonly name = 'ProtocolError';
  readonly body: unknown;

  constructor(message: string, body?: unknown) {
    super(message);
    this.body = body;
  }
}

/** Throws {@link ProtocolError} unless `value` is a valid `201 Created` body. */
export function parseCreateSessionBody(value: unknown): CreateSessionBody {
  const record = asRecord(value);
  const subscriptionsUrl = record?.['subscriptions_url'];
  const streamUrl = record?.['stream_url'];
  const expiresAt = record?.['expires_at'];

  if (typeof subscriptionsUrl !== 'string' || !isAbsoluteUrl(subscriptionsUrl)) {
    throw new ProtocolError('Session response is missing an absolute subscriptions_url', value);
  }
  if (typeof streamUrl !== 'string' || !isAbsoluteUrl(streamUrl)) {
    throw new ProtocolError('Session response is missing an absolute stream_url', value);
  }
  if (typeof expiresAt !== 'string' || Number.isNaN(Date.parse(expiresAt))) {
    throw new ProtocolError('Session response is missing a valid expires_at', value);
  }

  const wildcardSupport = record?.['wildcard_support'];
  if (wildcardSupport !== undefined && typeof wildcardSupport !== 'boolean') {
    throw new ProtocolError('Session response wildcard_support is not a boolean', value);
  }

  const heartbeat = record?.['heartbeat_interval'];
  if (
    heartbeat !== undefined &&
    (typeof heartbeat !== 'number' || !Number.isInteger(heartbeat) || heartbeat <= 0)
  ) {
    throw new ProtocolError('Session response heartbeat_interval is not a positive integer', value);
  }

  return {
    subscriptions_url: subscriptionsUrl,
    stream_url: streamUrl,
    expires_at: expiresAt,
    ...(heartbeat === undefined ? {} : { heartbeat_interval: heartbeat }),
    wildcard_support: wildcardSupport ?? true,
  };
}

/** Throws {@link ProtocolError} unless `value` is a `{ topics: [...] }` body. */
export function parseTopicsBody(value: unknown): TopicsBody {
  const topics = asRecord(value)?.['topics'];
  if (!Array.isArray(topics) || !topics.every((topic) => typeof topic === 'string')) {
    throw new ProtocolError('Expected a JSON body of the form { "topics": [ … ] }', value);
  }
  return { topics };
}

/**
 * Extracts an RFC 9457 problem detail body when the server sent one.
 *
 * Parsing is lenient about `title` and `detail` but requires `type` and `status`,
 * the two members a client branches on.
 */
export function parseProblemDetails(value: unknown): ProblemDetails | undefined {
  const record = asRecord(value);
  if (record === undefined) return undefined;

  const type = record['type'];
  const status = record['status'];
  const title = record['title'];
  const detail = record['detail'];

  if (typeof type !== 'string' || type.length === 0) return undefined;
  if (typeof status !== 'number' || !Number.isInteger(status)) return undefined;

  return {
    type,
    status,
    title: typeof title === 'string' ? title : httpStatusPhrase(status),
    detail: typeof detail === 'string' ? detail : '',
  };
}

export function asRecord(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

export function isAbsoluteUrl(value: string): boolean {
  try {
    return URL.canParse(value);
  } catch {
    return false;
  }
}
