/**
 * Server entry point: the service, its pluggable stores, the event hub, SSE
 * framing, and the cookie/CORS helpers an adapter needs.
 */

export {
  DEFAULT_HEARTBEAT_INTERVAL_MS,
  DEFAULT_MAX_LIVE_TICKETS_PER_PRINCIPAL,
  DEFAULT_MAX_SUBSCRIPTIONS,
  DEFAULT_SESSION_RATE_LIMIT,
  DEFAULT_TICKET_TTL_SECONDS,
  createStreamingService,
} from './service.js';
export type { StreamingService } from './service.js';
export type * from './types.js';

// Problem details are protocol-level, shared with the client subpath.
export { PROBLEM_MEDIA_TYPE, PROBLEM_TYPES, httpStatusPhrase, problem } from '../protocol.js';
export type { ProblemType } from '../protocol.js';

export { DEFAULT_QUEUE_LIMIT, EventHub } from './hub.js';
export type {
  EventHubOptions,
  PublishOptions,
  StreamEvent,
  StreamNextResult,
  StreamSubscription,
} from './hub.js';

export { InMemorySubscriptionStore } from './subscriptions.js';
export type { SubscriptionStore } from './subscriptions.js';

export { InMemoryTicketStore, hasExpired, issueTicket } from './tickets.js';
export type {
  InMemoryTicketStoreOptions,
  IssueTicketOptions,
  TicketRecord,
  TicketStore,
} from './tickets.js';

export { SSE_CONTENT_TYPE, SSE_HEADERS, formatSseFrame, heartbeatFrame } from './sse.js';
export type { SseFrame } from './sse.js';

export { DEFAULT_TICKET_COOKIE, clearCookie, parseCookies, serializeCookie } from './cookies.js';
export type { CookieOptions, SameSitePolicy } from './cookies.js';

export { evaluateCors, preflightHeaders } from './cors.js';
export type { CorsDecision, CorsPolicy, PreflightRequest } from './cors.js';

export { firstHeaderValue, readHeader } from './headers.js';
export { generateEventId, generateTicket, randomHex, sha256Hex } from './crypto.js';
