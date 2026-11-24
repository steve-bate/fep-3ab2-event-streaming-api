/**
 * Wire types for the server subpath.
 *
 * The service is HTTP-shaped but framework-agnostic: adapters hand it the
 * authenticated principal, the request headers and the browser `Origin`, and get
 * back a status, headers and a JSON body (or an SSE frame source). Nothing here
 * imports a web framework or a Node module.
 */

import type { CookieOptions } from './cookies.js';
import type { CorsPolicy } from './cors.js';
import type { SubscriptionStore } from './subscriptions.js';
import type { TicketStore } from './tickets.js';
import type { TopicRules } from '../topics/index.js';
import type { CreateSessionBody, ProblemDetails, TopicsBody } from '../protocol.js';

// The request/response bodies are protocol-level types, defined once for both
// subpaths; re-exported here so adapter code can import from one place.
export type { CreateSessionBody, ProblemDetails, TopicsBody };

/** A single header value, as Node's `IncomingHttpHeaders` represents it. */
export type HeaderValue = string | readonly string[] | undefined;
export type HeadersLike = Readonly<Record<string, HeaderValue>>;
export type HttpHeaders = Record<string, string | string[]>;

export interface ServiceSuccess<T> {
  readonly ok: true;
  readonly status: number;
  readonly headers: HttpHeaders;
  readonly body: T;
}

export interface ServiceFailure {
  readonly ok: false;
  readonly status: number;
  readonly headers: HttpHeaders;
  readonly body: ProblemDetails;
}

/**
 * Every control-endpoint call returns one of these, so an adapter can branch on
 * `ok` instead of guessing what a status code means.
 */
export type ServiceResult<T> = ServiceSuccess<T> | ServiceFailure;

/** What the host application must supply for every streaming request. */
export interface AuthenticatedRequest {
  /** URI of the authenticated ActivityPub actor, resolved by the host app. */
  readonly principal: string;
  /** Request headers; used for the ticket cookie and (optionally) the host. */
  readonly headers: HeadersLike;
  readonly origin?: string;
  /** Aborted when the client disconnects; ends the stream. */
  readonly signal?: AbortSignal;
}

export type StreamCloseReason =
  | 'client-disconnected'
  | 'superseded'
  | 'session-revoked'
  | 'ticket-expired'
  | 'server-closed'
  | 'error';

export interface OpenStreamSuccess {
  readonly ok: true;
  readonly status: 200;
  readonly headers: HttpHeaders;
  /** Wire-ready `text/event-stream` chunks: write each one and flush. */
  readonly frames: AsyncIterable<string>;
  readonly close: (reason: StreamCloseReason) => void;
}

export interface OpenStreamFailure {
  readonly ok: false;
  readonly status: 401 | 403 | 406 | 429 | 500;
  readonly headers: HttpHeaders;
  readonly body: ProblemDetails;
}

export type StreamResult = OpenStreamSuccess | OpenStreamFailure;

export interface StreamingLogger {
  debug(message: string, details?: Record<string, unknown>): void;
  info(message: string, details?: Record<string, unknown>): void;
  warn(message: string, details?: Record<string, unknown>): void;
}

/** Paths (relative to `publicOrigin`) advertised in the session response. */
export interface StreamingUrls {
  readonly control?: string;
  readonly subscriptions?: string;
  readonly stream?: string;
}

/**
 * Decides whether a Principal may observe a topic. A `POST {subscriptions_url}`
 * naming a topic this rejects is
 * answered with `403` and the c180 `principal-not-authorized` problem type.
 */
export type TopicAuthorization = (principal: string, topic: string) => boolean | Promise<boolean>;

/** Fixed-window rate limit on `POST {streamingControl}`. */
export interface SessionRateLimit {
  readonly max: number;
  readonly windowSeconds: number;
}

export interface TicketSettings {
  /** Lifetime of a ticket; the stream never outlives it. Default 300s. */
  readonly ttlSeconds?: number;
  readonly cookie?: Partial<CookieOptions>;
  /**
   * Live tickets kept per Principal; issuing one beyond the limit revokes the
   * oldest. Default 10; `0` disables the limit.
   */
  readonly maxPerPrincipal?: number;
  /**
   * Sessions allowed per Principal per window, answered with `429` when
   * exceeded. Default 60 per 60s; `false` disables.
   */
  readonly rateLimit?: SessionRateLimit | false;
}

export interface StreamingServiceOptions {
  /**
   * Absolute origin of this API, e.g. `https://api.example`. Session responses
   * must carry absolute URLs; when omitted the origin
   * is derived from `x-forwarded-proto` / `x-forwarded-host` / `host`, which is
   * only safe behind a proxy you control.
   */
  readonly publicOrigin?: string;
  readonly urls?: StreamingUrls;
  readonly ticket?: TicketSettings;
  /**
   * Topic policy. `maxSubscriptions` caps the per-principal set; `authorize`
   * rejects topics the Principal may not observe.
   */
  readonly topics?: TopicRules & {
    readonly maxSubscriptions?: number;
    readonly authorize?: TopicAuthorization;
  };
  /** Interval between `event: heartbeat` frames. Default 20s, 0 disables. */
  readonly heartbeatIntervalMs?: number;
  /** Buffered frames per subscriber before the oldest are dropped. Default 256. */
  readonly queueLimit?: number;
  /** Strict CORS allowlist; omit for same-origin-only deployments. */
  readonly cors?: CorsPolicy;
  /**
   * Answers for origins the allowlist does not name, such as the origin a client
   * registered before a Principal approved it. Runs only after the allowlist
   * declines, and only when the request carried an `Origin`.
   */
  readonly originTrust?: OriginTrust;
  readonly ticketStore?: TicketStore;
  readonly subscriptionStore?: SubscriptionStore;
  readonly logger?: StreamingLogger;
}

/**
 * Whether an `Origin` the allowlist does not name may call the API: the
 * client-bound path from the Deployment Origins section.
 */
export type OriginTrust = (
  origin: string,
  request: AuthenticatedRequest,
) => boolean | Promise<boolean>;

export interface ResolvedStreamingServiceOptions {
  readonly publicOrigin: string | undefined;
  readonly urls: Required<StreamingUrls>;
  readonly ticketTtlSeconds: number;
  readonly cookie: CookieOptions;
  readonly heartbeatIntervalMs: number;
  readonly queueLimit: number;
  readonly allowWildcards: boolean;
  readonly maxTopicLength: number;
  readonly maxSubscriptions: number;
  readonly maxLiveTicketsPerPrincipal: number;
  readonly sessionRateLimit: SessionRateLimit | undefined;
  readonly authorizeTopic: TopicAuthorization | undefined;
  readonly cors: CorsPolicy | undefined;
  readonly originTrust: OriginTrust | undefined;
}
