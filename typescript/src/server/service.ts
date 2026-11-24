/**
 * The streaming service: FEP-3ab2's control API and SSE endpoint as plain
 * functions over request-shaped inputs.
 *
 * This module owns the protocol; adapters own the framework. A request arrives
 * as `{ principal, headers, origin, signal }`, and the result is a status plus
 * headers plus a body, or for the stream endpoint an async source of wire-ready
 * SSE chunks. Nothing here imports Express, Fastify or `node:http`, so the whole
 * API is testable without sockets.
 */

import { TopicError, validateTopic, DEFAULT_TOPIC_MAX_LENGTH } from '../topics/index.js';
import { PROBLEM_MEDIA_TYPE, PROBLEM_TYPES, problem } from '../protocol.js';
import {
  DEFAULT_TICKET_COOKIE,
  clearCookie,
  parseCookies,
  serializeCookie,
  type CookieOptions,
} from './cookies.js';
import {
  evaluateCors,
  preflightHeaders,
  trustedOriginHeaders,
  type CorsDecision,
  type PreflightRequest,
} from './cors.js';
import { sha256Hex } from './crypto.js';
import { EventHub, DEFAULT_QUEUE_LIMIT, type PublishOptions } from './hub.js';
import { firstHeaderValue, readHeader } from './headers.js';
import { SSE_HEADERS, formatSseFrame, heartbeatFrame } from './sse.js';
import { InMemorySubscriptionStore, type SubscriptionStore } from './subscriptions.js';
import { InMemoryTicketStore, issueTicket, type TicketStore } from './tickets.js';
import type {
  AuthenticatedRequest,
  CreateSessionBody,
  HeadersLike,
  HttpHeaders,
  OpenStreamFailure,
  OpenStreamSuccess,
  ResolvedStreamingServiceOptions,
  ServiceFailure,
  ServiceResult,
  ServiceSuccess,
  SessionRateLimit,
  StreamCloseReason,
  StreamResult,
  StreamingServiceOptions,
  StreamingUrls,
  TopicsBody,
} from './types.js';

export const DEFAULT_TICKET_TTL_SECONDS = 300;
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 20_000;
export const DEFAULT_MAX_SUBSCRIPTIONS = 100;
export const DEFAULT_MAX_LIVE_TICKETS_PER_PRINCIPAL = 10;
export const DEFAULT_SESSION_RATE_LIMIT: SessionRateLimit = { max: 60, windowSeconds: 60 };

export interface StreamingService {
  readonly options: ResolvedStreamingServiceOptions;
  readonly ticketStore: TicketStore;
  readonly subscriptionStore: SubscriptionStore;

  createSession(request: AuthenticatedRequest): Promise<ServiceResult<CreateSessionBody>>;
  revokeSession(request: AuthenticatedRequest): Promise<ServiceResult<null>>;
  /** `GET {subscriptions_url}` */
  listSubscriptions(request: AuthenticatedRequest): Promise<ServiceResult<TopicsBody>>;
  /** `POST {subscriptions_url}` */
  addSubscriptions(
    request: AuthenticatedRequest & { readonly body: unknown },
  ): Promise<ServiceResult<TopicsBody>>;
  /** `DELETE {subscriptions_url}?topic=…` */
  removeSubscription(
    request: AuthenticatedRequest & { readonly topic: string | undefined },
  ): Promise<ServiceResult<null>>;
  /** `GET {stream_url}`: single-use ticket, multiplexed SSE feed */
  openStream(request: AuthenticatedRequest): Promise<StreamResult>;

  preflight(request?: PreflightRequest): ServiceResult<null>;
  cors(origin?: string): CorsDecision;

  /** Publishes an ActivityPub entity to every matching subscriber. */
  publish(topic: string, payload: unknown, options?: PublishOptions): Promise<number>;
  /** Drops expired ticket records. Call from a timer in production. */
  pruneExpiredTickets(now?: Date): Promise<number>;
  /** Ends every open stream; call on shutdown. */
  closeAll(): void;
}

interface ActiveStream {
  readonly ticket: string;
  readonly ticketKey: string;
  readonly principal: string;
  readonly close: (reason: StreamCloseReason) => void;
}

/** A live ticket, tracked so the per-principal limit can revoke the oldest. */
interface IssuedTicket {
  readonly ticket: string;
  readonly expiresAt: number;
}

/** Creates a streaming service. All collaborators are injectable. */
export function createStreamingService(options: StreamingServiceOptions = {}): StreamingService {
  const resolved = resolveOptions(options);
  const logger = options.logger;
  const ticketStore = options.ticketStore ?? new InMemoryTicketStore();
  const subscriptionStore = options.subscriptionStore ?? new InMemorySubscriptionStore();
  const hub = new EventHub({
    subscriptions: subscriptionStore,
    queueLimit: resolved.queueLimit,
    ...(logger === undefined ? {} : { logger }),
  });

  const activeByTicket = new Map<string, ActiveStream>();
  const activeByPrincipal = new Map<string, ActiveStream>();
  /** Tickets issued per Principal, oldest first */
  const issuedByPrincipal = new Map<string, IssuedTicket[]>();
  const issuanceTimes = new Map<string, number[]>();

  const validationRules = (allowWildcards: boolean) => ({
    allowWildcards,
    maxLength: resolved.maxTopicLength,
  });

  function ok<T>(status: number, body: T, headers: HttpHeaders = {}): ServiceSuccess<T> {
    return { ok: true, status, headers, body };
  }

  function fail(
    status: number,
    type: string,
    detail: string,
    headers: HttpHeaders = {},
  ): ServiceFailure {
    return {
      ok: false,
      status,
      headers: { 'content-type': PROBLEM_MEDIA_TYPE, ...headers },
      body: problem(type, status, detail),
    };
  }

  function streamFail(
    status: OpenStreamFailure['status'],
    type: string,
    detail: string,
    headers: HttpHeaders = {},
  ): OpenStreamFailure {
    return {
      ok: false,
      status,
      headers: { 'content-type': PROBLEM_MEDIA_TYPE, ...headers },
      body: problem(type, status, detail),
    };
  }

  /**
   * CORS headers when the request's origin is trusted, `undefined` when it is not.
   * The allowlist answers first; `originTrust` covers origins the server cannot
   * know in advance.
   */
  async function corsHeaders(
    request: AuthenticatedRequest,
    ticketOrigin?: string,
  ): Promise<HttpHeaders | undefined> {
    const origin = request.origin;
    if (origin === undefined || origin === '') return {};
    const decision = evaluateCors(origin, resolved.cors);

    // The stream request carries no credential of its own, so the ticket is the
    // evidence: an origin the ticket was issued to is trusted.
    if (ticketOrigin !== undefined && originMatchesTicket(origin, ticketOrigin)) {
      return trustedOriginHeaders(origin, resolved.cors);
    }

    // No hook: the allowlist is the whole answer, and no policy means no check.
    if (resolved.originTrust === undefined) {
      return decision.allowed ? decision.headers : undefined;
    }
    // An allowlisted origin short-circuits; everything else goes to the hook.
    if (decision.allowed && resolved.cors !== undefined) return decision.headers;
    if (!(await resolved.originTrust(origin, request))) return undefined;
    return trustedOriginHeaders(origin, resolved.cors);
  }

  function ticketFromCookie(request: AuthenticatedRequest) {
    const header = readHeader(request.headers, 'cookie');
    if (header === undefined) return undefined;
    return parseCookies(header)[resolved.cookie.name];
  }

  async function requireTicket(
    request: AuthenticatedRequest,
    cors: HttpHeaders,
  ): Promise<{ ok: true; ticket: string } | { ok: false; failure: ServiceFailure }> {
    const ticket = ticketFromCookie(request);
    if (ticket === undefined) {
      return {
        ok: false,
        failure: fail(
          401,
          PROBLEM_TYPES.invalidTicket,
          'A streaming ticket cookie is required',
          cors,
        ),
      };
    }

    const record = await ticketStore.read(ticket);
    if (record === null) {
      return {
        ok: false,
        failure: fail(
          401,
          PROBLEM_TYPES.invalidTicket,
          'The streaming ticket is invalid or expired',
          cors,
        ),
      };
    }
    if (record.principal !== request.principal) {
      return {
        ok: false,
        failure: fail(
          401,
          PROBLEM_TYPES.invalidTicket,
          'The streaming ticket was issued to a different principal',
          cors,
        ),
      };
    }

    return { ok: true, ticket };
  }

  function resolveOrigin(request: AuthenticatedRequest): string | undefined {
    if (resolved.publicOrigin !== undefined) return resolved.publicOrigin;

    const host = firstHeaderValue(
      readHeader(request.headers, 'x-forwarded-host') ?? readHeader(request.headers, 'host'),
    );
    if (host === undefined || host.length === 0) return undefined;

    const proto =
      firstHeaderValue(readHeader(request.headers, 'x-forwarded-proto')) ??
      (resolved.cookie.secure ? 'https' : 'http');
    if (proto !== 'http' && proto !== 'https') return undefined;

    return `${proto}://${host}`;
  }

  /** Parses a `{ topics: string[] }` body. Returns undefined when malformed. */
  function parseTopics(body: unknown) {
    if (typeof body !== 'object' || body === null) return undefined;
    const topics = (body as { topics?: unknown }).topics;
    if (!Array.isArray(topics)) return undefined;
    if (!topics.every((topic): topic is string => typeof topic === 'string')) return undefined;
    return topics;
  }

  async function revokeTicket(ticket: string) {
    await ticketStore.revoke(ticket);
    const key = await sha256Hex(ticket);
    activeByTicket.get(key)?.close('session-revoked');
  }

  function forgetIssued(principal: string, ticket: string) {
    const entries = issuedByPrincipal.get(principal);
    if (entries === undefined) return;
    const remaining = entries.filter((entry) => entry.ticket !== ticket);
    if (remaining.length === 0) issuedByPrincipal.delete(principal);
    else issuedByPrincipal.set(principal, remaining);
  }

  /**
   * Seconds to wait before this Principal may create another session, or
   * `undefined` when the request is within the limit.
   */
  function rateLimitRetryAfter(principal: string, now: number) {
    const limit = resolved.sessionRateLimit;
    if (limit === undefined) return undefined;

    const windowMs = limit.windowSeconds * 1000;
    const recent = (issuanceTimes.get(principal) ?? []).filter((at) => now - at < windowMs);
    issuanceTimes.set(principal, recent);

    if (recent.length < limit.max) return undefined;
    const oldest = recent[0] ?? now;
    return Math.max(1, Math.ceil((windowMs - (now - oldest)) / 1000));
  }

  function recordIssuance(principal: string, ticket: string, expiresAt: Date, now: number) {
    const entries = issuedByPrincipal.get(principal) ?? [];
    entries.push({ ticket, expiresAt: expiresAt.getTime() });
    issuedByPrincipal.set(principal, entries);

    const windowMs = (resolved.sessionRateLimit?.windowSeconds ?? 60) * 1000;
    const times = (issuanceTimes.get(principal) ?? []).filter((at) => now - at < windowMs);
    times.push(now);
    issuanceTimes.set(principal, times);
  }

  /**
   * Keeps the number of live tickets per Principal at the configured cap by
   * revoking the oldest, which also ends any stream it authorized.
   */
  async function enforceLiveTicketLimit(principal: string, now: number) {
    const max = resolved.maxLiveTicketsPerPrincipal;
    if (max <= 0) return;

    const live = (issuedByPrincipal.get(principal) ?? []).filter((entry) => entry.expiresAt > now);
    while (live.length >= max) {
      const oldest = live.shift();
      if (oldest === undefined) break;
      await revokeTicket(oldest.ticket);
      logger?.info('Revoked the oldest ticket beyond the per-principal limit', { principal });
    }
    if (live.length === 0) issuedByPrincipal.delete(principal);
    else issuedByPrincipal.set(principal, live);
  }

  const service: StreamingService = {
    options: resolved,
    ticketStore,
    subscriptionStore,

    async createSession(request: AuthenticatedRequest): Promise<ServiceResult<CreateSessionBody>> {
      const cors = await corsHeaders(request);
      if (cors === undefined) {
        return fail(403, PROBLEM_TYPES.originNotAllowed, 'The request origin is not allowed');
      }

      const origin = resolveOrigin(request);
      if (origin === undefined) {
        return fail(
          500,
          PROBLEM_TYPES.aboutBlank,
          'The public origin is unknown; configure `publicOrigin` on the service',
          cors,
        );
      }

      const now = Date.now();
      const retryAfter = rateLimitRetryAfter(request.principal, now);
      if (retryAfter !== undefined) {
        return fail(
          429,
          PROBLEM_TYPES.rateLimitExceeded,
          `Too many streaming sessions; retry in ${retryAfter} seconds`,
          { ...cors, 'retry-after': String(retryAfter) },
        );
      }

      await enforceLiveTicketLimit(request.principal, now);

      const { ticket, record } = issueTicket({
        principal: request.principal,
        ttlSeconds: resolved.ticketTtlSeconds,
        ...(request.origin === undefined ? {} : { origin: request.origin }),
      });
      await ticketStore.create(ticket, record);
      recordIssuance(request.principal, ticket, record.expiresAt, now);

      const absolute = (path: string): string => new URL(path, origin).toString();
      // Advertised only when the server actually sends heartbeats, and rounded up
      // so the promise ("no silence longer than this") stays true.
      const heartbeatSeconds =
        resolved.heartbeatIntervalMs > 0
          ? Math.ceil(resolved.heartbeatIntervalMs / 1000)
          : undefined;

      const body: CreateSessionBody = {
        subscriptions_url: absolute(resolved.urls.subscriptions),
        stream_url: absolute(resolved.urls.stream),
        expires_at: isoTimestamp(record.expiresAt),
        ...(heartbeatSeconds === undefined ? {} : { heartbeat_interval: heartbeatSeconds }),
        wildcard_support: resolved.allowWildcards,
      };

      const headers: HttpHeaders = {
        'content-type': 'application/json; charset=utf-8',
        'cache-control': 'no-store',
        'set-cookie': serializeCookie(ticket, {
          ...resolved.cookie,
          maxAgeSeconds: resolved.ticketTtlSeconds,
        }),
        ...cors,
      };

      logger?.info('Issued streaming session', {
        principal: request.principal,
        expiresAt: body.expires_at,
      });

      return ok(201, body, headers);
    },

    async revokeSession(request: AuthenticatedRequest): Promise<ServiceResult<null>> {
      const cors = await corsHeaders(request);
      if (cors === undefined) {
        return fail(403, PROBLEM_TYPES.originNotAllowed, 'The request origin is not allowed');
      }

      const ticket = ticketFromCookie(request);
      if (ticket !== undefined) {
        const record = await ticketStore.read(ticket);
        if (record !== null && record.principal === request.principal) {
          await revokeTicket(ticket);
          forgetIssued(request.principal, ticket);
          logger?.info('Revoked streaming session', { principal: request.principal });
        }
      }

      const headers: HttpHeaders = {
        'set-cookie': clearCookie(resolved.cookie),
        'cache-control': 'no-store',
        ...cors,
      };
      return ok(204, null, headers);
    },

    async listSubscriptions(request: AuthenticatedRequest): Promise<ServiceResult<TopicsBody>> {
      const cors = await corsHeaders(request);
      if (cors === undefined) {
        return fail(403, PROBLEM_TYPES.originNotAllowed, 'The request origin is not allowed');
      }
      const guard = await requireTicket(request, cors);
      if (!guard.ok) return { ...guard.failure, headers: { ...guard.failure.headers, ...cors } };

      const topics = await subscriptionStore.list(request.principal);
      return ok(200, { topics }, { 'content-type': 'application/json; charset=utf-8', ...cors });
    },

    async addSubscriptions(
      request: AuthenticatedRequest & { readonly body: unknown },
    ): Promise<ServiceResult<TopicsBody>> {
      const cors = await corsHeaders(request);
      if (cors === undefined) {
        return fail(403, PROBLEM_TYPES.originNotAllowed, 'The request origin is not allowed');
      }
      const guard = await requireTicket(request, cors);
      if (!guard.ok) return { ...guard.failure, headers: { ...guard.failure.headers, ...cors } };

      if (!isJsonBody(request.headers)) {
        return fail(
          415,
          PROBLEM_TYPES.aboutBlank,
          'The request body must be sent as application/json',
          cors,
        );
      }

      const requested = parseTopics(request.body);
      if (requested === undefined) {
        return fail(
          400,
          PROBLEM_TYPES.aboutBlank,
          'Expected a JSON body of the form { "topics": [ … ] }',
          cors,
        );
      }

      let validated: string[];
      try {
        validated = requested.map((topic) =>
          validateTopic(topic, validationRules(resolved.allowWildcards)),
        );
      } catch (error) {
        if (error instanceof TopicError) {
          return fail(400, PROBLEM_TYPES.invalidTopic, error.message, cors);
        }
        throw error;
      }

      // A subscription never grants access to a topic: one the Principal may
      // not observe is rejected outright.
      const authorize = resolved.authorizeTopic;
      if (authorize !== undefined) {
        for (const topic of validated) {
          if (!(await authorize(request.principal, topic))) {
            return fail(
              403,
              PROBLEM_TYPES.principalNotAuthorized,
              `The principal may not observe "${topic}"`,
              cors,
            );
          }
        }
      }

      const existing = await subscriptionStore.list(request.principal);
      const additions = validated.filter((topic) => !existing.includes(topic));
      if (existing.length + additions.length > resolved.maxSubscriptions) {
        return fail(
          400,
          PROBLEM_TYPES.aboutBlank,
          `A principal may subscribe to at most ${resolved.maxSubscriptions} topics`,
          cors,
        );
      }

      const topics = await subscriptionStore.add(request.principal, validated);
      return ok(200, { topics }, { 'content-type': 'application/json; charset=utf-8', ...cors });
    },

    async removeSubscription(
      request: AuthenticatedRequest & { readonly topic: string | undefined },
    ): Promise<ServiceResult<null>> {
      const cors = await corsHeaders(request);
      if (cors === undefined) {
        return fail(403, PROBLEM_TYPES.originNotAllowed, 'The request origin is not allowed');
      }
      const guard = await requireTicket(request, cors);
      if (!guard.ok) return { ...guard.failure, headers: { ...guard.failure.headers, ...cors } };

      if (request.topic === undefined) {
        return fail(400, PROBLEM_TYPES.aboutBlank, 'The `topic` query parameter is required', cors);
      }

      let topic: string;
      try {
        topic = validateTopic(request.topic, validationRules(resolved.allowWildcards));
      } catch (error) {
        if (error instanceof TopicError)
          return fail(400, PROBLEM_TYPES.invalidTopic, error.message, cors);
        throw error;
      }

      await subscriptionStore.remove(request.principal, topic);
      return ok(204, null, { 'cache-control': 'no-store', ...cors });
    },

    async openStream(request: AuthenticatedRequest): Promise<StreamResult> {
      const ticket = ticketFromCookie(request);
      const record = ticket === undefined ? null : await ticketStore.read(ticket);

      const cors = await corsHeaders(request, record?.origin);
      if (cors === undefined) {
        return streamFail(403, PROBLEM_TYPES.originNotAllowed, 'The request origin is not allowed');
      }
      const decision = { headers: cors };

      if (!acceptsEventStream(request.headers)) {
        return streamFail(
          406,
          PROBLEM_TYPES.aboutBlank,
          'The request must accept text/event-stream',
          decision.headers,
        );
      }

      if (ticket === undefined) {
        return streamFail(
          401,
          PROBLEM_TYPES.invalidTicket,
          'A streaming ticket cookie is required',
          decision.headers,
        );
      }

      if (record === null) {
        return streamFail(
          401,
          PROBLEM_TYPES.invalidTicket,
          'The streaming ticket is invalid or expired',
          decision.headers,
        );
      }
      if (record.principal !== request.principal) {
        return streamFail(
          401,
          PROBLEM_TYPES.invalidTicket,
          'The streaming ticket was issued to a different principal',
          decision.headers,
        );
      }
      if (!originMatchesTicket(request.origin, record.origin)) {
        return streamFail(
          403,
          PROBLEM_TYPES.originNotAllowed,
          'The request origin does not match the origin recorded with the ticket',
          decision.headers,
        );
      }
      if (record.consumedAt !== undefined) {
        return streamFail(
          401,
          PROBLEM_TYPES.invalidTicket,
          'The ticket has already been used to open a stream',
          decision.headers,
        );
      }

      // Single use: the store decides, so two racing requests cannot both win.
      if ((await ticketStore.consume(ticket)) === null) {
        return streamFail(
          401,
          PROBLEM_TYPES.invalidTicket,
          'The ticket has already been used to open a stream',
          decision.headers,
        );
      }

      const ticketKey = await sha256Hex(ticket);
      const principal = request.principal;

      // One stream per principal: the newer connection replaces the older one,
      // and the ticket that authorized the older stream is revoked with it.
      const previous = activeByPrincipal.get(principal);
      if (previous !== undefined) {
        previous.close('superseded');
        await ticketStore.revoke(previous.ticket);
        forgetIssued(principal, previous.ticket);
        logger?.info('Revoked the ticket of the superseded stream', { principal });
      }

      const subscription = hub.subscribe(principal);
      let closed = false;
      let closeReason: StreamCloseReason | undefined;

      const active: ActiveStream = {
        ticket,
        ticketKey,
        principal,
        close: (reason: StreamCloseReason) => {
          if (closed) return;
          closed = true;
          closeReason = reason;
          subscription.close();
          if (activeByTicket.get(ticketKey) === active) activeByTicket.delete(ticketKey);
          if (activeByPrincipal.get(principal) === active) activeByPrincipal.delete(principal);
        },
      };
      activeByTicket.set(ticketKey, active);
      activeByPrincipal.set(principal, active);

      const onAbort = () => {
        active.close('client-disconnected');
      };
      request.signal?.addEventListener('abort', onAbort, { once: true });

      const expiresAt = record.expiresAt.getTime();
      const heartbeatMs = resolved.heartbeatIntervalMs;

      async function* frames(): AsyncGenerator<string> {
        let nextHeartbeatAt = heartbeatMs > 0 ? Date.now() + heartbeatMs : Number.POSITIVE_INFINITY;
        try {
          while (!closed) {
            const now = Date.now();
            if (now >= expiresAt) {
              active.close('ticket-expired');
              break;
            }

            const waitMs = Math.max(1, Math.min(expiresAt, nextHeartbeatAt) - now);
            const result = await subscription.next(waitMs);

            if (result.kind === 'event') {
              yield formatSseFrame({
                id: result.event.id,
                event: result.event.event,
                data: result.event.data,
              });
              continue;
            }
            if (result.kind === 'closed') break;

            if (Date.now() >= expiresAt) {
              active.close('ticket-expired');
              break;
            }
            if (Date.now() >= nextHeartbeatAt) {
              yield formatSseFrame(heartbeatFrame());
              nextHeartbeatAt = Date.now() + heartbeatMs;
            }
          }
        } finally {
          active.close('server-closed');
          request.signal?.removeEventListener('abort', onAbort);
          logger?.info('Closed stream', {
            principal,
            reason: closeReason ?? 'unknown',
            dropped: subscription.dropped,
          });
        }
      }

      const stream: OpenStreamSuccess = {
        ok: true,
        status: 200,
        headers: { ...SSE_HEADERS, ...decision.headers },
        frames: frames(),
        close: active.close,
      };

      logger?.info('Opened stream', { principal, expiresAt: isoTimestamp(record.expiresAt) });
      return stream;
    },

    preflight(request: PreflightRequest = {}): ServiceResult<null> {
      const decision = evaluateCors(request.origin, resolved.cors);
      // An adapter that already ran `originTrust` passes the answer, so a
      // client-bound origin gets the same preflight headers as an allowlisted one.
      let headers: HttpHeaders;
      if (decision.allowed) {
        headers = decision.headers;
      } else if (resolved.originTrust !== undefined && request.origin !== undefined) {
        // A preflight never carries the credential, so it cannot know whether the
        // origin is bound to a client. Answer it, and let the real request decide:
        // granting a preflight executes nothing.
        headers = trustedOriginHeaders(request.origin, resolved.cors);
      } else {
        return fail(403, PROBLEM_TYPES.originNotAllowed, 'The request origin is not allowed');
      }
      return ok(
        204,
        null,
        preflightHeaders(
          { allowed: true, origin: request.origin, headers },
          resolved.cors,
          request,
        ),
      );
    },

    cors(origin?: string): CorsDecision {
      return evaluateCors(origin, resolved.cors);
    },

    async publish(
      topic: string,
      payload: unknown,
      publishOptions: PublishOptions = {},
    ): Promise<number> {
      return hub.publish(topic, payload, publishOptions);
    },

    async pruneExpiredTickets(now: Date = new Date()): Promise<number> {
      const removed = await ticketStore.prune(now);
      const at = now.getTime();
      for (const [principal, entries] of issuedByPrincipal) {
        const live = entries.filter((entry) => entry.expiresAt > at);
        if (live.length === 0) issuedByPrincipal.delete(principal);
        else issuedByPrincipal.set(principal, live);
      }
      return removed;
    },

    closeAll(): void {
      for (const stream of activeByTicket.values()) stream.close('server-closed');
      activeByTicket.clear();
      activeByPrincipal.clear();
      issuedByPrincipal.clear();
      issuanceTimes.clear();
      hub.closeAll();
    },
  };

  return service;
}

function resolveOptions(options: StreamingServiceOptions): ResolvedStreamingServiceOptions {
  const urls: Required<StreamingUrls> = {
    control: normalizePath(options.urls?.control ?? '/sse/control'),
    subscriptions: normalizePath(options.urls?.subscriptions ?? '/sse/control/subscriptions'),
    stream: normalizePath(options.urls?.stream ?? '/sse/stream'),
  };

  const cookieDefaults: CookieOptions = {
    ...DEFAULT_TICKET_COOKIE,
    // Scope the ticket to the streaming API, not the whole origin, unless told otherwise.
    path: commonPathPrefix([urls.control, urls.subscriptions, urls.stream]),
  };
  const cookie: CookieOptions = { ...cookieDefaults, ...options.ticket?.cookie };
  // Browsers drop a SameSite=None cookie that is not Secure, so the ticket would
  // never come back and every stream would answer 401.
  if (cookie.sameSite === 'None' && !cookie.secure) {
    throw new RangeError('ticket.cookie.sameSite "None" requires secure: true');
  }

  const ttlSeconds = options.ticket?.ttlSeconds ?? DEFAULT_TICKET_TTL_SECONDS;
  if (!Number.isFinite(ttlSeconds) || ttlSeconds <= 0) {
    throw new RangeError('ticket.ttlSeconds must be a positive number');
  }

  const heartbeatIntervalMs = options.heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
  if (heartbeatIntervalMs < 0) {
    throw new RangeError('heartbeatIntervalMs must not be negative');
  }

  const maxLiveTickets = options.ticket?.maxPerPrincipal ?? DEFAULT_MAX_LIVE_TICKETS_PER_PRINCIPAL;
  if (!Number.isInteger(maxLiveTickets) || maxLiveTickets < 0) {
    throw new RangeError('ticket.maxPerPrincipal must be a non-negative integer');
  }

  const rateLimitOption = options.ticket?.rateLimit;
  const sessionRateLimit: SessionRateLimit | undefined =
    rateLimitOption === false ? undefined : (rateLimitOption ?? DEFAULT_SESSION_RATE_LIMIT);
  if (sessionRateLimit !== undefined) {
    const { max, windowSeconds } = sessionRateLimit;
    if (!Number.isInteger(max) || max <= 0) {
      throw new RangeError('ticket.rateLimit.max must be a positive integer');
    }
    if (!Number.isFinite(windowSeconds) || windowSeconds <= 0) {
      throw new RangeError('ticket.rateLimit.windowSeconds must be a positive number');
    }
  }

  const publicOrigin =
    options.publicOrigin === undefined ? undefined : normalizeOrigin(options.publicOrigin);

  return {
    publicOrigin,
    urls,
    ticketTtlSeconds: Math.trunc(ttlSeconds),
    cookie,
    heartbeatIntervalMs: Math.trunc(heartbeatIntervalMs),
    queueLimit: options.queueLimit ?? DEFAULT_QUEUE_LIMIT,
    allowWildcards: options.topics?.allowWildcards ?? true,
    maxTopicLength: options.topics?.maxLength ?? DEFAULT_TOPIC_MAX_LENGTH,
    maxSubscriptions: options.topics?.maxSubscriptions ?? DEFAULT_MAX_SUBSCRIPTIONS,
    maxLiveTicketsPerPrincipal: maxLiveTickets,
    sessionRateLimit,
    authorizeTopic: options.topics?.authorize,
    cors: options.cors,
    originTrust: options.originTrust,
  };
}

/**
 * True when the request's `Accept` names `text/event-stream`.
 *
 * The spec makes that exact media type the condition for serving the stream, so
 * a wildcard `Accept` (what `curl` sends by default) is answered `406`
 * rather than left open.
 */
function acceptsEventStream(headers: HeadersLike) {
  const accept = readHeader(headers, 'accept');
  if (accept === undefined) return false;

  return accept.split(',').some((range) => {
    const media = (range.split(';')[0] ?? '').trim().toLowerCase();
    return media === 'text/event-stream';
  });
}

/**
 * The subscriptions endpoint takes a JSON body and nothing else; the spec answers
 * any other media type with `415`.
 */
function isJsonBody(headers: HeadersLike) {
  const contentType = readHeader(headers, 'content-type');
  if (contentType === undefined) return false;

  const media = (contentType.split(';')[0] ?? '').trim().toLowerCase();
  return media === 'application/json';
}

/**
 * The stream request has to come from the origin the ticket was issued to, when
 * it names one. A request without an `Origin` is a non-browser client; one that
 * names an origin a ticket never recorded is refused.
 */
function originMatchesTicket(requestOrigin: string | undefined, ticketOrigin: string | undefined) {
  if (requestOrigin === undefined || requestOrigin === '') return true;
  if (ticketOrigin === undefined) return false;
  return normalizeOrigin(requestOrigin) === normalizeOrigin(ticketOrigin);
}

function normalizePath(path: string) {
  if (!path.startsWith('/')) {
    throw new TypeError(`Streaming URL paths must start with "/": ${path}`);
  }
  return path.length > 1 ? path.replace(/\/+$/, '') : '/';
}

function normalizeOrigin(origin: string) {
  let url: URL;
  try {
    url = new URL(origin);
  } catch {
    throw new TypeError(`publicOrigin must be an absolute URL: ${origin}`);
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new TypeError(`publicOrigin must be http or https: ${origin}`);
  }
  return url.origin;
}

/** Longest shared path prefix of the streaming endpoints, as a cookie `Path`. */
function commonPathPrefix(paths: readonly string[]) {
  const splitPaths = paths.map((path) => path.split('/').filter((segment) => segment.length > 0));
  const first = splitPaths[0] ?? [];
  const common: string[] = [];

  for (const [index, segment] of first.entries()) {
    if (!splitPaths.every((path) => path[index] === segment)) break;
    common.push(segment);
  }

  return common.length === 0 ? '/' : `/${common.join('/')}`;
}

/** `2026-03-10T12:05:00Z`: the shape the spec's examples use. */
function isoTimestamp(date: Date) {
  return date.toISOString().replace(/\.\d{3}Z$/, 'Z');
}
