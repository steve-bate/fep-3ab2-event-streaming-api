/**
 * `StreamingClient`: the browser (or Node) side of FEP-3ab2.
 *
 * The client owns the session lifecycle: it asks for a ticket, opens one SSE
 * connection for all subscriptions, renews the ticket before it expires, and
 * reconnects with a *fresh* ticket when the connection dies. That last part is
 * why the browser's automatic `EventSource` retry is switched off: an SSE
 * reconnect would replay a single-use ticket and be rejected with `401`.
 */

import { normalizeEscapes } from '../topics/index.js';
import { StreamingClientError, toClientError } from './errors.js';
import {
  addSubscriptions,
  createStreamingSession,
  discoverStreamingControl,
  listSubscriptions,
  removeSubscription,
  revokeStreamingSession,
  type SessionClientContext,
} from './session.js';
import type {
  EventSourceFactory,
  EventSourceLike,
  FetchLike,
  MessageEventLike,
  ReconnectOptions,
  StreamingActivity,
  StreamingClientEvents,
  StreamingClientOptions,
  StreamingClientState,
  StreamingClientStatus,
  StreamingSession,
  ClientLogger,
} from './types.js';

type TimerHandle = ReturnType<typeof setTimeout>;

const DEFAULT_RECONNECT: Required<ReconnectOptions> = {
  initialDelayMs: 500,
  maxDelayMs: 15_000,
  maxAttempts: Number.POSITIVE_INFINITY,
  factor: 2,
  jitter: 0.3,
};

const DEFAULT_RENEW_BEFORE_EXPIRY_MS = 5_000;

/**
 * The watchdog deadline for a session: twice the advertised heartbeat interval,
 * which is the FEP's own failure detector. No interval advertised
 * means no deadline; silence then surfaces only as a transport error.
 */
function watchdogFor(session: { readonly heartbeatIntervalSeconds: number | undefined }) {
  const seconds = session.heartbeatIntervalSeconds;
  return seconds === undefined ? 0 : seconds * 1_000 * 2;
}

export interface OpenOptions {
  /** Topics to (re)apply before connecting; defaults to the last known set. */
  readonly topics?: readonly string[];
}

export class StreamingClient {
  readonly #controlUrl: string;
  readonly #fetchFn: FetchLike;
  readonly #credentials: RequestCredentials;
  readonly #withCredentials: boolean;
  readonly #eventSourceFactory: EventSourceFactory;
  /** Configured watchdog deadline; `undefined` derives it from the session. */
  readonly #heartbeatTimeoutMs: number | undefined;
  readonly #renewBeforeExpiryMs: number;
  readonly #reconnectOptions: Required<ReconnectOptions>;
  readonly #headers: Readonly<Record<string, string>> | undefined;
  readonly #logger: ClientLogger | undefined;

  readonly #listeners: { [K in keyof StreamingClientEvents]: Set<StreamingClientEvents[K]> } = {
    state: new Set(),
    session: new Set(),
    activity: new Set(),
    heartbeat: new Set(),
    subscriptions: new Set(),
    error: new Set(),
  };

  #state: StreamingClientState = 'idle';
  #session: StreamingSession | undefined;
  #source: EventSourceLike | undefined;
  #topics: string[] = [];
  #watchdogMs = 0;
  #attempt = 0;
  #generation = 0;
  #closed = false;

  #timers: {
    heartbeat: TimerHandle | undefined;
    renew: TimerHandle | undefined;
    reconnect: TimerHandle | undefined;
  } = { heartbeat: undefined, renew: undefined, reconnect: undefined };

  readonly #handlers = {
    open: (): void => {
      this.#onOpen();
    },
    error: (): void => {
      this.#onStreamError();
    },
    activity: (event: MessageEventLike): void => {
      this.#onActivity(event);
    },
    heartbeat: (event: MessageEventLike): void => {
      this.#resetHeartbeatWatchdog();
      this.#emit('heartbeat', { receivedAt: new Date(), data: parseData(event.data) });
    },
  };

  constructor(options: StreamingClientOptions) {
    if (!URL.canParse(options.streamingControl)) {
      throw new StreamingClientError(
        `streamingControl must be an absolute URL: ${options.streamingControl}`,
        { code: 'protocol' },
      );
    }

    this.#controlUrl = options.streamingControl;
    this.#fetchFn = options.fetch ?? defaultFetch;
    this.#credentials = options.credentials ?? 'include';
    this.#withCredentials = options.withCredentials ?? true;
    this.#eventSourceFactory = options.eventSourceFactory ?? defaultEventSourceFactory;
    this.#heartbeatTimeoutMs = options.heartbeatTimeoutMs;
    this.#renewBeforeExpiryMs = options.renewBeforeExpiryMs ?? DEFAULT_RENEW_BEFORE_EXPIRY_MS;
    this.#reconnectOptions = { ...DEFAULT_RECONNECT, ...options.reconnect };
    this.#headers = options.headers;
    this.#logger = options.logger;
    this.#topics = normalizeTopics(options.topics ?? []);
  }

  get status(): StreamingClientStatus {
    return {
      state: this.#state,
      session: this.#session,
      topics: [...this.#topics],
      reconnectAttempt: this.#attempt,
    };
  }

  get state(): StreamingClientState {
    return this.#state;
  }

  get session(): StreamingSession | undefined {
    return this.#session;
  }

  on<K extends keyof StreamingClientEvents>(
    event: K,
    handler: StreamingClientEvents[K],
  ): () => void {
    this.#listeners[event].add(handler);
    return () => {
      this.#listeners[event].delete(handler);
    };
  }

  /** Resolves once the stream is open. */
  async open(options: OpenOptions = {}): Promise<StreamingSession> {
    if (this.#closed) {
      throw new StreamingClientError('The client has been closed', { code: 'closed' });
    }
    if (options.topics !== undefined) this.#topics = normalizeTopics(options.topics);
    if (this.#state === 'open' && this.#session !== undefined) return this.#session;

    return this.#connect(true);
  }

  /** Closes the stream and, by default, revokes the session server-side. */
  async close(options: { readonly revoke?: boolean } = {}): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    this.#generation += 1;
    this.#clearTimers();
    this.#detachStream();

    const session = this.#session;
    this.#session = undefined;
    this.#setState('closed');

    if ((options.revoke ?? true) && session !== undefined) {
      try {
        await revokeStreamingSession(this.#context());
      } catch (error) {
        this.#emitFailure(toClientError(error));
      }
    }
  }

  /** Adds topics and returns the server's full set. */
  async subscribe(topics: string | readonly string[]): Promise<readonly string[]> {
    const session = this.#requireSession();
    const additions = typeof topics === 'string' ? [topics] : [...topics];
    if (additions.length === 0) return this.#topics;

    const result = await addSubscriptions(this.#context(), session.subscriptionsUrl, additions);
    this.#setTopics(result);
    return this.#topics;
  }

  /** Removes a topic and returns the server's full set. */
  async unsubscribe(topic: string): Promise<readonly string[]> {
    const session = this.#requireSession();
    await removeSubscription(this.#context(), session.subscriptionsUrl, topic);
    const result = await listSubscriptions(this.#context(), session.subscriptionsUrl);
    this.#setTopics(result);
    return this.#topics;
  }

  async refreshSubscriptions(): Promise<readonly string[]> {
    const session = this.#requireSession();
    const topics = await listSubscriptions(this.#context(), session.subscriptionsUrl);
    this.#setTopics(topics);
    return this.#topics;
  }

  // internals

  async #connect(resetAttempts: boolean): Promise<StreamingSession> {
    if (this.#closed) {
      throw new StreamingClientError('The client has been closed', { code: 'closed' });
    }
    if (resetAttempts) this.#attempt = 0;

    const generation = this.#generation;
    this.#setState(this.#attempt === 0 ? 'connecting' : 'reconnecting');
    this.#clearTimer('heartbeat');
    this.#clearTimer('renew');
    this.#clearTimer('reconnect');
    this.#detachStream();

    const session = await createStreamingSession(this.#context());
    this.#assertCurrent(generation);

    this.#session = session;
    this.#emit('session', session);

    // Subscriptions live on the server, per principal, but a restarted server has
    // none: re-applying the mirror makes reconnect self-healing, and it is
    // idempotent by the spec.
    if (this.#topics.length > 0) {
      const topics = await addSubscriptions(
        this.#context(),
        session.subscriptionsUrl,
        this.#topics,
      );
      this.#assertCurrent(generation);
      this.#setTopics(topics);
    }

    this.#attachStream(session);
    this.#armRenewal(session);
    this.#attempt = 0;
    return session;
  }

  async #reconnect(): Promise<void> {
    try {
      await this.#connect(false);
    } catch (error) {
      if (this.#closed) return;
      this.#emitFailure(toClientError(error));
      this.#scheduleReconnect();
    }
  }

  /** Renews ahead of `expires_at`: a new ticket, then a new stream on it. */
  async #renew(): Promise<void> {
    if (this.#closed) return;
    this.#log('debug', 'Renewing streaming session');
    try {
      await this.#connect(false);
    } catch (error) {
      this.#emitFailure(toClientError(error));
      this.#scheduleReconnect();
    }
  }

  #attachStream(session: StreamingSession): void {
    this.#watchdogMs = this.#heartbeatTimeoutMs ?? watchdogFor(session);

    const source = this.#eventSourceFactory(session.streamUrl, {
      withCredentials: this.#withCredentials,
    });
    this.#source = source;

    source.addEventListener('open', this.#handlers.open);
    source.addEventListener('error', this.#handlers.error);
    source.addEventListener('activitypub', this.#handlers.activity);
    source.addEventListener('heartbeat', this.#handlers.heartbeat);
    // A server that sends unnamed frames produces `message` events instead.
    source.addEventListener('message', this.#handlers.activity);
  }

  #detachStream(): void {
    const source = this.#source;
    if (source === undefined) return;
    this.#source = undefined;

    source.removeEventListener('open', this.#handlers.open);
    source.removeEventListener('error', this.#handlers.error);
    source.removeEventListener('activitypub', this.#handlers.activity);
    source.removeEventListener('heartbeat', this.#handlers.heartbeat);
    source.removeEventListener('message', this.#handlers.activity);
    source.close();
  }

  #onOpen(): void {
    this.#attempt = 0;
    this.#setState('open');
    this.#resetHeartbeatWatchdog();
  }

  #onStreamError(): void {
    if (this.#closed) return;
    this.#emitFailure(
      new StreamingClientError('The event stream reported an error', { code: 'transport' }),
    );
    // Close rather than let EventSource retry: the ticket it holds is spent.
    this.#detachStream();
    this.#scheduleReconnect();
  }

  #onActivity(event: MessageEventLike): void {
    this.#resetHeartbeatWatchdog();
    const data = parseData(event.data);
    const id =
      event.lastEventId !== undefined && event.lastEventId !== '' ? event.lastEventId : idOf(data);
    const activity: StreamingActivity = { id, event: 'activitypub', data, receivedAt: new Date() };
    this.#emit('activity', activity);
  }

  #resetHeartbeatWatchdog(): void {
    const deadline = this.#watchdogMs;
    if (deadline <= 0) return;
    this.#clearTimer('heartbeat');
    this.#timers.heartbeat = setTimeout(() => {
      this.#timers.heartbeat = undefined;
      if (this.#closed) return;
      this.#emitFailure(
        new StreamingClientError(`No event or heartbeat within ${deadline} ms`, {
          code: 'transport',
        }),
      );
      this.#detachStream();
      this.#scheduleReconnect();
    }, deadline);
  }

  #armRenewal(session: StreamingSession): void {
    this.#clearTimer('renew');
    const delay = Math.max(0, session.expiresAt.getTime() - Date.now() - this.#renewBeforeExpiryMs);
    this.#timers.renew = setTimeout(() => {
      this.#timers.renew = undefined;
      void this.#renew();
    }, delay);
  }

  #scheduleReconnect(): void {
    if (this.#closed || this.#timers.reconnect !== undefined) return;

    if (this.#attempt >= this.#reconnectOptions.maxAttempts) {
      this.#emitFailure(
        new StreamingClientError('Reconnect attempts exhausted', { code: 'transport' }),
      );
      this.#setState('closed');
      return;
    }

    const delay = this.#backoffDelay(this.#attempt);
    this.#attempt += 1;
    this.#setState('reconnecting');
    this.#timers.reconnect = setTimeout(() => {
      this.#timers.reconnect = undefined;
      void this.#reconnect();
    }, delay);
  }

  #backoffDelay(attempt: number): number {
    const { initialDelayMs, maxDelayMs, factor, jitter } = this.#reconnectOptions;
    const base = Math.min(maxDelayMs, initialDelayMs * Math.pow(factor, attempt));
    const spread = 1 + jitter * (Math.random() * 2 - 1);
    return Math.max(0, Math.round(base * spread));
  }

  #context(): SessionClientContext {
    return {
      controlUrl: this.#controlUrl,
      fetch: this.#fetchFn,
      credentials: this.#credentials,
      ...(this.#headers === undefined ? {} : { headers: this.#headers }),
    };
  }

  #requireSession(): StreamingSession {
    if (this.#session === undefined) {
      throw new StreamingClientError('No streaming session; call open() first', {
        code: 'not-connected',
      });
    }
    return this.#session;
  }

  #assertCurrent(generation: number): void {
    if (this.#closed || generation !== this.#generation) {
      throw new StreamingClientError('Connection superseded by a newer one', { code: 'closed' });
    }
  }

  #setTopics(topics: readonly string[]): void {
    const normalized = normalizeTopics(topics);
    if (normalized.join('\n') === this.#topics.join('\n')) return;
    this.#topics = normalized;
    this.#emit('subscriptions', this.#topics);
  }

  #setState(next: StreamingClientState): void {
    if (next === this.#state) return;
    const previous = this.#state;
    this.#state = next;
    this.#log('debug', 'State changed', { from: previous, to: next });
    this.#emit('state', next, previous);
  }

  #emit<K extends keyof StreamingClientEvents>(
    event: K,
    ...args: Parameters<StreamingClientEvents[K]>
  ): void {
    for (const handler of [...this.#listeners[event]]) {
      try {
        // The mapped listener type is a function of exactly these parameters.
        (handler as (...values: Parameters<StreamingClientEvents[K]>) => void)(...args);
      } catch (error) {
        this.#log('error', 'A listener threw', { event, error: String(error) });
      }
    }
  }

  #emitFailure(error: StreamingClientError): void {
    this.#log('warn', error.message, { code: error.code, status: error.status });
    this.#emit('error', error);
  }

  #log(
    level: 'debug' | 'info' | 'warn' | 'error',
    message: string,
    details?: Record<string, unknown>,
  ): void {
    this.#logger?.(level, message, details);
  }

  #clearTimer(name: 'heartbeat' | 'renew' | 'reconnect'): void {
    const timer = this.#timers[name];
    if (timer === undefined) return;
    clearTimeout(timer);
    this.#timers[name] = undefined;
  }

  #clearTimers(): void {
    this.#clearTimer('heartbeat');
    this.#clearTimer('renew');
    this.#clearTimer('reconnect');
  }
}

export function discoverStreamingEndpoint(
  actorUri: string,
  options: {
    readonly fetch?: FetchLike;
    readonly credentials?: RequestCredentials;
    readonly headers?: Readonly<Record<string, string>>;
  } = {},
): Promise<string> {
  return discoverStreamingControl(actorUri, {
    fetch: options.fetch ?? defaultFetch,
    credentials: options.credentials ?? 'include',
    ...(options.headers === undefined ? {} : { headers: options.headers }),
  });
}

const defaultFetch: FetchLike = (url, init) => globalThis.fetch(url, init);

const defaultEventSourceFactory: EventSourceFactory = (url, init) => {
  const Constructor = (globalThis as { EventSource?: typeof EventSource }).EventSource;
  if (Constructor === undefined) {
    throw new StreamingClientError(
      'No global EventSource; pass `eventSourceFactory` (e.g. eventsource in Node)',
      { code: 'transport' },
    );
  }
  return new Constructor(url, init);
};

function normalizeTopics(topics: readonly string[]) {
  const unique = new Set<string>();
  for (const topic of topics) {
    const trimmed = topic.trim();
    if (trimmed.length > 0) unique.add(normalizeEscapes(trimmed));
  }
  return [...unique];
}

function parseData(data: unknown) {
  if (typeof data !== 'string') return data;
  try {
    return JSON.parse(data) as unknown;
  } catch {
    return data;
  }
}

function idOf(data: unknown) {
  if (typeof data === 'object' && data !== null) {
    const id = (data as { id?: unknown }).id;
    if (typeof id === 'string') return id;
  }
  return '';
}
