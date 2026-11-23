/**
 * Test doubles for FEP-3ab2: an in-process loopback (service + client over a
 * fake `fetch` / `EventSource`), a controllable `EventSource`, and an SSE parser.
 */

export { createFakeEventSource, createEventSourceRecorder } from './fake-event-source.js';
export type {
  EventSourceRecorder,
  FakeEventSource,
  FakeEventSourceState,
} from './fake-event-source.js';

export { createLoopback } from './loopback.js';
export type { Loopback, LoopbackOptions } from './loopback.js';

export { SseParser, parseSseText } from './sse-parser.js';
export type { ParsedSseEvent } from './sse-parser.js';

/** Resolves once `predicate` is true, polling on the microtask/macrotask queue. */
export async function waitFor(
  predicate: () => boolean,
  options: { readonly timeoutMs?: number; readonly intervalMs?: number } = {},
): Promise<void> {
  const timeoutMs = options.timeoutMs ?? 1_000;
  const intervalMs = options.intervalMs ?? 1;
  const deadline = Date.now() + timeoutMs;

  while (!predicate()) {
    if (Date.now() > deadline) throw new Error('waitFor() timed out');
    await new Promise<void>((resolve) => {
      setTimeout(resolve, intervalMs);
    });
  }
}

/** Lets pending promise callbacks run without advancing the clock. */
export async function flushMicrotasks(rounds = 4): Promise<void> {
  for (let index = 0; index < rounds; index += 1) {
    await Promise.resolve();
  }
}
