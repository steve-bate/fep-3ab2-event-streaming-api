export type StreamingClientState = 'idle' | 'connecting' | 'open' | 'reconnecting' | 'closed';

/** What `POST {streamingControl}` returned */
export interface StreamingSession {
  /** Absolute subscriptions endpoint; use it as-is. */
  readonly subscriptionsUrl: string;
  /** Absolute SSE endpoint; use it as-is. */
  readonly streamUrl: string;
  /** When the ticket (and therefore the stream) expires */
  readonly expiresAt: Date;
  /**
   * Maximum seconds the server will go without sending a frame, when it
   * advertised one. `undefined` means the server sends
   * none, so a silent connection is only visible as a transport error.
   */
  readonly heartbeatIntervalSeconds: number | undefined;
  readonly wildcardSupport: boolean;
}

export interface StreamingActivity {
  /** SSE `id` field: the payload's `id`, or a server-generated id. */
  readonly id: string;
  /** SSE event name; `activitypub` for entity frames. */
  readonly event: string;
  readonly data: unknown;
  readonly receivedAt: Date;
}

export interface StreamingHeartbeat {
  readonly receivedAt: Date;
  readonly data: unknown;
}

export interface MessageEventLike {
  readonly data: unknown;
  readonly lastEventId?: string;
}

/**
 * The slice of `EventSource` this library reads. Substituting the global
 * `EventSource` with something else (a Node implementation, a fake in tests) is
 * the one seam that makes the client testable without a browser.
 */
export interface EventSourceLike {
  addEventListener(type: string, listener: (event: MessageEventLike) => void): void;
  removeEventListener(type: string, listener: (event: MessageEventLike) => void): void;
  close(): void;
}

export type EventSourceFactory = (
  url: string,
  init?: { readonly withCredentials?: boolean },
) => EventSourceLike;

/** Standard `fetch`, narrowed to what the client calls. */
export type FetchLike = (url: string, init: RequestInit) => Promise<Response>;

export interface ReconnectOptions {
  /** First retry delay. Default 500 ms. */
  readonly initialDelayMs?: number;
  /** Ceiling for the exponential backoff. Default 15 s. */
  readonly maxDelayMs?: number;
  /** Retries before giving up; `Infinity` (the default) retries forever. */
  readonly maxAttempts?: number;
  /** Backoff multiplier. Default 2. */
  readonly factor?: number;
  /** Fraction of random spread applied to each delay. Default 0.3. */
  readonly jitter?: number;
}

export type ClientLogger = (
  level: 'debug' | 'info' | 'warn' | 'error',
  message: string,
  details?: Record<string, unknown>,
) => void;

export interface StreamingClientOptions {
  /** `endpoints.streamingControl` from the actor document */
  readonly streamingControl: string;
  /** Defaults to the global `fetch`. */
  readonly fetch?: FetchLike;
  /** `fetch` credentials mode. Default `include`, so the ticket cookie travels. */
  readonly credentials?: RequestCredentials;
  /** Passed to `EventSource`; required for cross-origin cookies. Default `true`. */
  readonly withCredentials?: boolean;
  /** Defaults to the global `EventSource`. */
  readonly eventSourceFactory?: EventSourceFactory;
  /**
   * Treat a silent stream as dead after this long. Defaults to twice the
   * advertised `heartbeat_interval`; when the server advertises none there is no
   * watchdog and silence surfaces only as a transport error. `0` disables it.
   */
  readonly heartbeatTimeoutMs?: number;
  /** Renew this long before `expires_at`. Default 5 s; `0` renews at expiry. */
  readonly renewBeforeExpiryMs?: number;
  readonly reconnect?: ReconnectOptions;
  /** Topics applied on every new session */
  readonly topics?: readonly string[];
  /** Extra headers for control-API calls, such as `authorization`. */
  readonly headers?: Readonly<Record<string, string>>;
  readonly logger?: ClientLogger;
}

export interface StreamingClientEvents {
  state: (state: StreamingClientState, previous: StreamingClientState) => void;
  session: (session: StreamingSession) => void;
  activity: (activity: StreamingActivity) => void;
  heartbeat: (heartbeat: StreamingHeartbeat) => void;
  subscriptions: (topics: readonly string[]) => void;
  error: (error: Error) => void;
}

export interface StreamingClientStatus {
  readonly state: StreamingClientState;
  readonly session: StreamingSession | undefined;
  readonly topics: readonly string[];
  /** Consecutive failed connection attempts */
  readonly reconnectAttempt: number;
}
