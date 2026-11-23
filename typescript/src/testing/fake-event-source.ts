/**
 * A controllable stand-in for the browser's `EventSource`.
 *
 * Tests drive it directly (`open()`, `emit()`, `fail()`), and the in-process
 * loopback uses it to replay the frames the real service produced.
 */

import type { EventSourceFactory, EventSourceLike, MessageEventLike } from '../client/types.js';

export type FakeEventSourceState = 'connecting' | 'open' | 'closed';

export interface FakeEventSource extends EventSourceLike {
  readonly url: string;
  readonly withCredentials: boolean;
  readonly readyState: FakeEventSourceState;
  open(): void;
  /** Dispatches `error` without closing, like a dropped connection. */
  fail(): void;
  /** Dispatches a named event carrying `data` (objects are JSON-encoded). */
  emit(event: string, data?: unknown, options?: { readonly id?: string }): void;
  onClose(listener: () => void): () => void;
}

export function createFakeEventSource(
  url = 'http://localhost/stream',
  init?: { readonly withCredentials?: boolean },
): FakeEventSource {
  const listeners = new Map<string, Set<(event: MessageEventLike) => void>>();
  const closeListeners = new Set<() => void>();
  let readyState: FakeEventSourceState = 'connecting';

  function dispatch(type: string, data: unknown, id?: string) {
    const message: MessageEventLike = id === undefined ? { data } : { data, lastEventId: id };
    for (const listener of [...(listeners.get(type) ?? [])]) listener(message);
  }

  return {
    url,
    withCredentials: init?.withCredentials ?? false,

    get readyState(): FakeEventSourceState {
      return readyState;
    },

    addEventListener(type: string, listener: (event: MessageEventLike) => void): void {
      const set = listeners.get(type) ?? new Set();
      set.add(listener);
      listeners.set(type, set);
    },

    removeEventListener(type: string, listener: (event: MessageEventLike) => void): void {
      listeners.get(type)?.delete(listener);
    },

    close(): void {
      if (readyState === 'closed') return;
      readyState = 'closed';
      for (const listener of [...closeListeners]) listener();
    },

    open(): void {
      if (readyState !== 'connecting') return;
      readyState = 'open';
      dispatch('open', undefined);
    },

    fail(): void {
      dispatch('error', undefined);
    },

    emit(event: string, data?: unknown, options?: { readonly id?: string }): void {
      const payload = typeof data === 'string' ? data : JSON.stringify(data);
      dispatch(event, payload, options?.id);
    },

    onClose(listener: () => void): () => void {
      closeListeners.add(listener);
      return () => {
        closeListeners.delete(listener);
      };
    },
  };
}

export interface EventSourceRecorder {
  readonly factory: EventSourceFactory;
  /** Every source created through the factory, in order */
  readonly instances: FakeEventSource[];
  readonly last: FakeEventSource | undefined;
}

/** An `EventSourceFactory` that records the sources the client created. */
export function createEventSourceRecorder(): EventSourceRecorder {
  const instances: FakeEventSource[] = [];
  return {
    instances,
    get last(): FakeEventSource | undefined {
      return instances.at(-1);
    },
    factory: (url, init) => {
      const source = createFakeEventSource(url, init);
      instances.push(source);
      return source;
    },
  };
}
