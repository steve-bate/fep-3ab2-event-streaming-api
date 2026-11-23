/**
 * The event hub: fan-out from publishers to per-principal streams.
 *
 * A publication is matched against each subscribed principal's patterns at send
 * time, so a subscription added mid-stream takes effect immediately. Each open
 * stream owns a bounded queue; when a client cannot keep up the oldest frame is
 * dropped rather than growing memory without limit, and the drop count is
 * visible to the caller.
 */

import { compileTopicPattern, validateTopic, type TopicPattern } from '../topics/index.js';
import { generateEventId } from './crypto.js';
import type { SubscriptionStore } from './subscriptions.js';
import type { StreamingLogger } from './types.js';

export interface StreamEvent {
  readonly id: string;
  readonly event: string;
  readonly data: unknown;
}

export type StreamNextResult =
  | { readonly kind: 'event'; readonly event: StreamEvent }
  | { readonly kind: 'timeout' }
  | { readonly kind: 'closed' };

/** A single open stream's view of the hub. One consumer only. */
export interface StreamSubscription {
  /** Waits up to `timeoutMs` for the next frame */
  next(timeoutMs: number): Promise<StreamNextResult>;
  close(): void;
  /** Frames discarded because the consumer fell behind */
  readonly dropped: number;
}

export interface PublishOptions {
  /** SSE event name; defaults to `activitypub`. */
  readonly event?: string;
}

export interface EventHubOptions {
  readonly subscriptions: SubscriptionStore;
  readonly queueLimit?: number;
  readonly logger?: StreamingLogger;
}

export const DEFAULT_QUEUE_LIMIT = 256;

export class EventHub {
  readonly #subscriptions: SubscriptionStore;
  readonly #queueLimit: number;
  readonly #logger: StreamingLogger | undefined;
  readonly #sinks = new Map<string, Set<BoundedQueue>>();
  readonly #patterns = new Map<string, TopicPattern | null>();

  constructor(options: EventHubOptions) {
    this.#subscriptions = options.subscriptions;
    this.#queueLimit = options.queueLimit ?? DEFAULT_QUEUE_LIMIT;
    this.#logger = options.logger;
  }

  /**
   * Publishes `payload` to every principal subscribed to a pattern matching
   * `topic`. Returns the number of streams the frame was queued for.
   *
   * `topic` is a concrete topic: wildcards are not allowed, so a publisher
   * escapes a literal `+` or `#`.
   */
  async publish(topic: string, payload: unknown, options: PublishOptions = {}): Promise<number> {
    const concrete = validateTopic(topic, { allowWildcards: false });
    const event: StreamEvent = {
      id: eventIdFor(payload),
      event: options.event ?? 'activitypub',
      data: payload,
    };

    let delivered = 0;
    for (const principal of await this.#subscriptions.principals()) {
      const sinks = this.#sinks.get(principal);
      if (sinks === undefined || sinks.size === 0) continue;

      const topics = await this.#subscriptions.list(principal);
      if (!topics.some((pattern) => this.#matches(pattern, concrete))) continue;

      for (const sink of sinks) {
        sink.push(event);
        delivered += 1;
      }
    }

    this.#logger?.debug('Published event', { topic: concrete, delivered });
    return delivered;
  }

  subscribe(principal: string): StreamSubscription {
    const queue = new BoundedQueue(this.#queueLimit);
    const sinks = this.#sinks.get(principal) ?? new Set<BoundedQueue>();
    sinks.add(queue);
    this.#sinks.set(principal, sinks);

    return {
      next: (timeoutMs: number) => queue.next(timeoutMs),
      close: () => {
        queue.close();
        sinks.delete(queue);
        if (sinks.size === 0) this.#sinks.delete(principal);
      },
      get dropped() {
        return queue.dropped;
      },
    };
  }

  closeAll(): void {
    for (const sinks of this.#sinks.values()) {
      for (const sink of sinks) sink.close();
    }
    this.#sinks.clear();
  }

  #matches(pattern: string, topic: string): boolean {
    let compiled = this.#patterns.get(pattern);
    if (compiled === undefined) {
      compiled = this.#compile(pattern);
      if (this.#patterns.size >= 1000) this.#patterns.clear();
      this.#patterns.set(pattern, compiled);
    }
    return compiled === null ? false : compiled.matches(topic);
  }

  #compile(pattern: string): TopicPattern | null {
    try {
      return compileTopicPattern(pattern);
    } catch (error) {
      // A pattern can only reach the store through validation, but a custom or
      // restored store may hold something else: skip it rather than fail a publish.
      this.#logger?.warn('Ignoring invalid subscription pattern', {
        pattern,
        error: error instanceof Error ? error.message : String(error),
      });
      return null;
    }
  }
}

function eventIdFor(payload: unknown) {
  if (typeof payload === 'object' && payload !== null) {
    const candidate = (payload as { id?: unknown }).id;
    if (typeof candidate === 'string' && candidate.length > 0) return candidate;
  }
  return generateEventId();
}

/**
 * A bounded FIFO with a single waiting consumer.
 *
 * `next(timeoutMs)` resolves with a frame, `timeout`, or `closed`; it never
 * leaves a promise hanging, so a heartbeat deadline cannot swallow an event.
 */
class BoundedQueue {
  readonly #items: StreamEvent[] = [];
  readonly #limit: number;
  #closed = false;
  #dropped = 0;
  #waiter: (() => void) | undefined;

  constructor(limit: number) {
    this.#limit = Math.max(1, Math.trunc(limit));
  }

  get dropped(): number {
    return this.#dropped;
  }

  push(event: StreamEvent): void {
    if (this.#closed) return;
    if (this.#items.length >= this.#limit) {
      this.#items.shift();
      this.#dropped += 1;
    }
    this.#items.push(event);
    this.#waiter?.();
  }

  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#waiter?.();
  }

  next(timeoutMs: number): Promise<StreamNextResult> {
    const buffered = this.#shift();
    if (buffered !== undefined) return Promise.resolve({ kind: 'event', event: buffered });
    if (this.#closed) return Promise.resolve({ kind: 'closed' });

    return new Promise<StreamNextResult>((resolve) => {
      let settled = false;

      const settle = (result: StreamNextResult) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        if (this.#waiter === wake) this.#waiter = undefined;
        resolve(result);
      };

      const wake = () => {
        const event = this.#shift();
        if (event !== undefined) settle({ kind: 'event', event });
        else if (this.#closed) settle({ kind: 'closed' });
      };

      const timer = setTimeout(
        () => {
          settle({ kind: 'timeout' });
        },
        Math.max(0, Math.trunc(timeoutMs)),
      );

      this.#waiter = wake;
    });
  }

  #shift(): StreamEvent | undefined {
    return this.#items.shift();
  }
}
