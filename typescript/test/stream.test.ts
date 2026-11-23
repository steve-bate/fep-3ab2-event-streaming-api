import { afterEach, describe, expect, it, vi } from 'vitest';
import { PROBLEM_TYPES, type OpenStreamSuccess } from '@fep-3ab2/streaming/server';
import {
  PRINCIPAL,
  buildRequest,
  jsonBody,
  nextFrame,
  nextFrames,
  testService,
  ticketCookie,
} from './helpers.js';
import type { StreamingService } from '@fep-3ab2/streaming/server';

interface OpenStream {
  readonly cookie: string;
  readonly stream: OpenStreamSuccess;
  readonly iterator: AsyncIterator<string>;
}

async function openStream(
  service: StreamingService,
  topics: readonly string[] = [],
  principal: string = PRINCIPAL,
): Promise<OpenStream> {
  const issued = await service.createSession(buildRequest(undefined, { principal }));
  const cookie = ticketCookie(issued.headers);

  if (topics.length > 0) {
    const added = await service.addSubscriptions({
      ...buildRequest(cookie, { principal }),
      body: { topics },
    });
    expect(added.ok).toBe(true);
  }

  const result = await service.openStream(buildRequest(cookie, { principal }));
  if (!result.ok) throw new Error(`openStream failed: ${result.status} ${result.body.type}`);

  return { cookie, stream: result, iterator: result.frames[Symbol.asyncIterator]() };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('GET {stream_url}', () => {
  it('requires a ticket cookie', async () => {
    const service = testService();
    const result = await service.openStream(buildRequest());

    expect(result.ok).toBe(false);
    expect(result.status).toBe(401);
  });

  it('rejects an unknown ticket', async () => {
    const service = testService();
    const result = await service.openStream(buildRequest('sse_ticket=nope'));
    expect(result.ok).toBe(false);
    expect(result.status).toBe(401);
  });

  it('requires Accept: text/event-stream', async () => {
    const service = testService();
    const issued = await service.createSession(buildRequest());
    const cookie = ticketCookie(issued.headers);

    const result = await service.openStream({ principal: PRINCIPAL, headers: { cookie } });

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.status).toBe(406);
    expect(result.body).toMatchObject({ type: PROBLEM_TYPES.aboutBlank, status: 406 });
  });

  it('rejects a ticket belonging to another principal', async () => {
    const service = testService();
    const issued = await service.createSession(buildRequest());
    const cookie = ticketCookie(issued.headers);

    const result = await service.openStream(
      buildRequest(cookie, { principal: 'https://api.example/users/bob' }),
    );
    expect(result.ok).toBe(false);
    expect(result.status).toBe(401);
  });

  it('opens an event stream with the headers the spec requires', async () => {
    const service = testService();
    const { stream } = await openStream(service);

    expect(stream.status).toBe(200);
    expect(stream.headers['content-type']).toContain('text/event-stream');
    expect(stream.headers['cache-control']).toContain('no-cache');
    expect(stream.headers['x-accel-buffering']).toBe('no');
    stream.close('client-disconnected');
  });

  it('delivers a published entity with its id and event name', async () => {
    const service = testService();
    const { stream, iterator } = await openStream(service, ['server.example/note/1']);

    await service.publish('server.example/note/1', {
      id: 'https://server.example/activities/abc123',
      type: 'Create',
    });

    const frame = await nextFrame(iterator);
    expect(frame).toContain('id: https://server.example/activities/abc123');
    expect(frame).toContain('event: activitypub');
    expect(jsonBody(frame)).toMatchObject({ type: 'Create' });
    stream.close('client-disconnected');
  });

  it('generates an id when the payload has none', async () => {
    const service = testService();
    const { stream, iterator } = await openStream(service, ['a/1']);

    await service.publish('a/1', { type: 'Note' });
    const frame = await nextFrame(iterator);

    expect(frame).toMatch(/^id: [0-9a-f]{32}\n/);
    stream.close('client-disconnected');
  });

  it('only delivers topics the principal subscribed to', async () => {
    const service = testService();
    const { stream, iterator } = await openStream(service, ['a/#']);

    await service.publish('b/1', { id: 'b-1' });
    await service.publish('a/1', { id: 'a-1' });

    const frame = await nextFrame(iterator);
    expect(frame).toContain('id: a-1');
    stream.close('client-disconnected');
  });

  it('lets a wildcard pattern cover nested topics', async () => {
    const service = testService();
    const { stream, iterator } = await openStream(service, ['server.example/note/#']);

    await service.publish('server.example/note/1/2/3', { id: 'deep' });
    expect(await nextFrame(iterator)).toContain('id: deep');
    stream.close('client-disconnected');
  });

  it('refuses to publish a wildcard topic', async () => {
    const service = testService();
    await expect(service.publish('a/#', { id: 'x' })).rejects.toThrow(/not supported/);
  });

  it('marks the ticket used, so a second stream is refused', async () => {
    const service = testService();
    const { cookie, stream } = await openStream(service);
    stream.close('client-disconnected');

    const second = await service.openStream(buildRequest(cookie));
    expect(second.ok).toBe(false);
    if (second.ok) return;
    expect(second.status).toBe(401);
    expect(second.body.type).toBe(PROBLEM_TYPES.invalidTicket);
  });

  it('keeps the ticket usable for subscription management after the stream opens', async () => {
    const service = testService();
    const { cookie, stream } = await openStream(service, ['a/1']);

    const added = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: ['a/2'] },
    });
    expect(added.ok).toBe(true);
    expect(added.body).toEqual({ topics: ['a/1', 'a/2'] });
    stream.close('client-disconnected');
  });

  it('supersedes the previous stream for the same principal', async () => {
    const service = testService();
    const first = await openStream(service, ['a/1']);
    const second = await openStream(service, ['a/1']);

    // The first stream ends without a frame.
    const ended = await first.iterator.next();
    expect(ended.done).toBe(true);

    await service.publish('a/1', { id: 'only-new' });
    expect(await nextFrame(second.iterator)).toContain('id: only-new');
    second.stream.close('client-disconnected');
  });

  it('revokes the ticket of the stream it supersedes', async () => {
    const service = testService();
    const first = await openStream(service, ['a/1']);
    await openStream(service, ['a/1']);

    // §Opening the Stream: the stream closed by the concurrency limit takes the
    // ticket that authorized it with it.
    const after = await service.listSubscriptions(buildRequest(first.cookie));
    expect(after.ok).toBe(false);
    if (after.ok) return;
    expect(after.status).toBe(401);
  });

  it('closes the stream when the session is revoked', async () => {
    const service = testService();
    const { cookie, iterator } = await openStream(service, ['a/1']);

    await service.revokeSession(buildRequest(cookie));
    const ended = await iterator.next();
    expect(ended.done).toBe(true);
  });

  it('closes the stream when the request is aborted', async () => {
    const service = testService();
    const controller = new AbortController();
    const issued = await service.createSession(buildRequest());
    const cookie = ticketCookie(issued.headers);
    const result = await service.openStream(buildRequest(cookie, { signal: controller.signal }));
    if (!result.ok) throw new Error('expected an open stream');

    const iterator = result.frames[Symbol.asyncIterator]();
    const pending = iterator.next();
    controller.abort();

    const ended = await pending;
    expect(ended.done).toBe(true);
  });

  it('closes every stream on shutdown', async () => {
    const service = testService();
    const { iterator } = await openStream(service, ['a/1']);

    const pending = iterator.next();
    service.closeAll();

    expect((await pending).done).toBe(true);
  });

  it('sends heartbeats on the configured interval', async () => {
    vi.useFakeTimers();
    const service = testService({ heartbeatIntervalMs: 1_000 });
    const { stream, iterator } = await openStream(service);

    const frame = await vi.waitFor(async () => {
      const pending = iterator.next();
      await vi.advanceTimersByTimeAsync(1_100);
      return pending;
    });

    expect(frame.done).toBe(false);
    expect(frame.value).toContain('event: heartbeat');
    stream.close('client-disconnected');
  });

  it('ends the stream when the ticket expires', async () => {
    vi.useFakeTimers();
    const service = testService({ ticket: { ttlSeconds: 60, cookie: { secure: false } } });
    const { stream, iterator } = await openStream(service);

    const pending = iterator.next();
    await vi.advanceTimersByTimeAsync(61_000);

    expect((await pending).done).toBe(true);
    stream.close('client-disconnected');
  });

  it('drops the oldest frames when a consumer falls behind', async () => {
    const service = testService({ queueLimit: 2 });
    const { stream, iterator } = await openStream(service, ['a/1']);

    for (const id of ['1', '2', '3', '4']) await service.publish('a/1', { id });

    const frames = await nextFrames(iterator, 2);
    expect(frames[0]).toContain('id: 3');
    expect(frames[1]).toContain('id: 4');
    stream.close('client-disconnected');
  });

  it('carries CORS headers on the stream response', async () => {
    const service = testService({ cors: { allowedOrigins: ['https://app.example'] } });
    const issued = await service.createSession(
      buildRequest(undefined, { origin: 'https://app.example' }),
    );
    const cookie = ticketCookie(issued.headers);
    const result = await service.openStream(
      buildRequest(cookie, { origin: 'https://app.example' }),
    );

    if (!result.ok) throw new Error('expected an open stream');
    expect(result.headers['access-control-allow-origin']).toBe('https://app.example');
    result.close('client-disconnected');
  });

  it('refuses a stream from a disallowed origin', async () => {
    const service = testService({ cors: { allowedOrigins: ['https://app.example'] } });
    const result = await service.openStream(
      buildRequest(undefined, { origin: 'https://evil.example' }),
    );

    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.status).toBe(403);
  });
});

describe('EventHub', () => {
  it('reports how many streams a publication reached', async () => {
    const service = testService();
    const { stream } = await openStream(service, ['a/1']);

    expect(await service.publish('a/1', { id: 'x' })).toBe(1);
    expect(await service.publish('a/2', { id: 'y' })).toBe(0);
    stream.close('client-disconnected');
  });

  it('prunes expired tickets on demand', async () => {
    const service = testService({ ticket: { ttlSeconds: 60, cookie: { secure: false } } });
    await service.createSession(buildRequest());

    expect(await service.pruneExpiredTickets(new Date(Date.now() + 120_000))).toBe(1);
  });
});
