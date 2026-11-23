/**
 * In-process loopback: a `fetch` + `EventSource` pair wired straight to a
 * `StreamingService`, with a cookie jar standing in for the browser.
 *
 * This is how the client and the server are tested *together* without a socket,
 * a port or a browser: `createStreamingService` + `createLoopback` +
 * `StreamingClient` exercises the whole protocol, ticket cookies included.
 */

import type { EventSourceFactory, FetchLike } from '../client/types.js';
import { PROBLEM_MEDIA_TYPE, PROBLEM_TYPES, problem } from '../protocol.js';
import type { ServiceResult } from '../server/types.js';
import type { StreamingService } from '../server/service.js';
import { createFakeEventSource, type FakeEventSource } from './fake-event-source.js';
import { SseParser } from './sse-parser.js';

export interface LoopbackOptions {
  readonly service: StreamingService;
  /** The principal the simulated browser is authenticated as. */
  readonly principal: string | (() => string);
  /** `Origin` header to send; omit to simulate a same-origin request. */
  readonly origin?: string;
  /** Extra headers on every request, e.g. `authorization`. */
  readonly headers?: Readonly<Record<string, string>>;
}

export interface Loopback {
  readonly fetch: FetchLike;
  readonly eventSourceFactory: EventSourceFactory;
  /** The `Cookie` header the simulated browser would send next */
  cookieHeader(): string;
  /** Every `EventSource` the client opened */
  readonly streams: FakeEventSource[];
}

export function createLoopback(options: LoopbackOptions): Loopback {
  const cookies = new Map<string, string>();
  const streams: FakeEventSource[] = [];
  const { urls } = options.service.options;

  const cookieHeader = () =>
    [...cookies.entries()].map(([name, value]) => `${name}=${value}`).join('; ');

  const principal = () =>
    typeof options.principal === 'function' ? options.principal() : options.principal;

  function rememberCookies(header: string | string[] | undefined) {
    if (header === undefined) return;
    for (const value of Array.isArray(header) ? header : [header]) {
      const [pair] = value.split(';');
      if (pair === undefined) continue;
      const separator = pair.indexOf('=');
      if (separator < 1) continue;
      const name = pair.slice(0, separator).trim();
      const raw = pair.slice(separator + 1).trim();
      const expires = /max-age=0/i.test(value);
      if (raw.length === 0 || expires) cookies.delete(name);
      else cookies.set(name, raw);
    }
  }

  function toResponse(result: ServiceResult<unknown>) {
    const headers = new Headers();
    for (const [name, value] of Object.entries(result.headers)) {
      if (name.toLowerCase() === 'set-cookie') {
        rememberCookies(value);
        continue;
      }
      for (const item of Array.isArray(value) ? value : [value]) headers.append(name, item);
    }

    if (result.body === null || result.body === undefined) {
      return new Response(null, { status: result.status, headers });
    }
    // The service decides the media type: `application/json` for bodies,
    // `application/problem+json` for errors.
    if (!headers.has('content-type')) headers.set('content-type', 'application/json');
    return new Response(JSON.stringify(result.body), { status: result.status, headers });
  }

  /** A problem detail for a failure the adapter itself detects (404, 405). */
  function problemResponse(status: number, detail: string) {
    return new Response(JSON.stringify(problem(PROBLEM_TYPES.aboutBlank, status, detail)), {
      status,
      headers: { 'content-type': PROBLEM_MEDIA_TYPE },
    });
  }

  const fetchImpl: FetchLike = async (input, init) => {
    const method = (init.method ?? 'GET').toUpperCase();
    const url = new URL(input);
    const request = {
      principal: principal(),
      headers: {
        ...options.headers,
        cookie: cookieHeader(),
      },
      ...(options.origin === undefined ? {} : { origin: options.origin }),
    };

    const json = typeof init.body === 'string' ? (JSON.parse(init.body) as unknown) : undefined;

    if (url.pathname === urls.control) {
      if (method === 'POST') return toResponse(await options.service.createSession(request));
      if (method === 'DELETE') return toResponse(await options.service.revokeSession(request));
      return problemResponse(405, `${method} is not allowed on the streaming control endpoint`);
    }

    if (url.pathname === urls.subscriptions) {
      if (method === 'GET') return toResponse(await options.service.listSubscriptions(request));
      if (method === 'POST') {
        return toResponse(await options.service.addSubscriptions({ ...request, body: json }));
      }
      if (method === 'DELETE') {
        return toResponse(
          await options.service.removeSubscription({
            ...request,
            topic: url.searchParams.get('topic') ?? undefined,
          }),
        );
      }
      return problemResponse(405, `${method} is not allowed on the subscriptions endpoint`);
    }

    return problemResponse(404, `No streaming endpoint at ${url.pathname}`);
  };

  const eventSourceFactory: EventSourceFactory = (streamUrl, init) => {
    const source = createFakeEventSource(streamUrl, init);
    streams.push(source);

    const controller = new AbortController();
    const unsubscribe = source.onClose(() => {
      controller.abort();
    });

    void (async () => {
      const result = await options.service.openStream({
        principal: principal(),
        // A real EventSource always sends this; the service answers 406 without it.
        headers: { ...options.headers, cookie: cookieHeader(), accept: 'text/event-stream' },
        signal: controller.signal,
        ...(options.origin === undefined ? {} : { origin: options.origin }),
      });

      unsubscribe();
      if (!result.ok) {
        source.fail();
        return;
      }

      source.open();
      const parser = new SseParser();
      try {
        for await (const chunk of result.frames) {
          for (const event of parser.push(chunk)) {
            source.emit(
              event.event ?? 'message',
              event.data,
              event.id === undefined ? undefined : { id: event.id },
            );
          }
        }
      } catch {
        // The consumer aborted the stream; `close()` already fired.
      } finally {
        if (source.readyState !== 'closed') source.fail();
      }
    })();

    return source;
  };

  return { fetch: fetchImpl, eventSourceFactory, cookieHeader, streams };
}
