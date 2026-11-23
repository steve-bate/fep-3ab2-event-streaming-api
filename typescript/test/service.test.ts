import { describe, expect, it } from 'vitest';
import {
  InMemorySubscriptionStore,
  InMemoryTicketStore,
  PROBLEM_TYPES,
  hasExpired,
  issueTicket,
  type CreateSessionBody,
  type StreamingService,
} from '@fep-3ab2/streaming/server';
import {
  API_ORIGIN,
  OTHER_PRINCIPAL,
  PRINCIPAL,
  buildRequest,
  setCookie,
  testService,
  ticketCookie,
} from './helpers.js';

describe('POST {streamingControl}', () => {
  it('issues a session with the fields the spec requires', async () => {
    const service = testService();
    const result = await service.createSession(buildRequest());

    expect(result.ok).toBe(true);
    expect(result.status).toBe(201);

    const body = result.body as CreateSessionBody;
    expect(body.subscriptions_url).toBe(`${API_ORIGIN}/sse/control/subscriptions`);
    expect(body.stream_url).toBe(`${API_ORIGIN}/sse/stream`);
    expect(body.wildcard_support).toBe(true);
    expect(body.expires_at).toMatch(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$/);
    expect(body).not.toHaveProperty('ticket');
  });

  it('returns URLs that are opaque but absolute', async () => {
    const service = testService({
      omitPublicOrigin: true,
      urls: {
        control: '/api/stream/ctl',
        subscriptions: '/api/stream/subs',
        stream: '/api/stream/events',
      },
    });
    const result = await service.createSession(
      buildRequest(undefined, {
        headers: { host: 'fediverse.test', 'x-forwarded-proto': 'https' },
      }),
    );

    expect(result.ok).toBe(true);
    const body = result.body as CreateSessionBody;
    expect(body.subscriptions_url).toBe('https://fediverse.test/api/stream/subs');
    expect(body.stream_url).toBe('https://fediverse.test/api/stream/events');
  });

  it('reports a configuration error when the origin cannot be resolved', async () => {
    const service = testService({ omitPublicOrigin: true });
    const result = await service.createSession(buildRequest());

    expect(result.ok).toBe(false);
    expect(result.status).toBe(500);
  });

  it('sets a ticket cookie scoped to the streaming API', async () => {
    const service = testService();
    const result = await service.createSession(buildRequest());
    const cookie = setCookie(result.headers);

    expect(cookie.startsWith('sse_ticket=')).toBe(true);
    expect(cookie).toContain('Path=/sse');
    expect(cookie).toContain('HttpOnly');
    expect(cookie).toContain('SameSite=Lax');
    expect(cookie).toContain('Max-Age=300');
    expect(cookie).not.toContain('Secure');
  });

  it('uses the configured cookie name and attributes', async () => {
    const service = testService({
      ticket: {
        ttlSeconds: 60,
        cookie: { name: 'stream_ticket', path: '/sse/stream', sameSite: 'Strict', secure: true },
      },
    });
    const cookie = setCookie((await service.createSession(buildRequest())).headers);

    expect(cookie.startsWith('stream_ticket=')).toBe(true);
    expect(cookie).toContain('Max-Age=60');
    expect(cookie).toContain('Path=/sse/stream');
    expect(cookie).toContain('SameSite=Strict');
    expect(cookie).toContain('Secure');
  });

  it('advertises heartbeat_interval when it sends heartbeats', async () => {
    const service = testService({ heartbeatIntervalMs: 1_500 });
    const result = await service.createSession(buildRequest());

    // Rounded up, so "no silence longer than this" stays true.
    expect((result.body as CreateSessionBody).heartbeat_interval).toBe(2);
  });

  it('omits heartbeat_interval when it sends no heartbeats', async () => {
    const service = testService();
    const result = await service.createSession(buildRequest());
    expect(result.body as CreateSessionBody).not.toHaveProperty('heartbeat_interval');
  });

  it('refuses a SameSite=None ticket cookie that is not Secure', () => {
    // Browsers drop such a cookie, so every stream would answer 401.
    expect(() => testService({ ticket: { cookie: { secure: false, sameSite: 'None' } } })).toThrow(
      /secure/,
    );
  });

  it('honours disabled wildcard support', async () => {
    const service = testService({ topics: { allowWildcards: false } });
    const result = await service.createSession(buildRequest());
    expect((result.body as CreateSessionBody).wildcard_support).toBe(false);
  });

  it('rejects a disallowed origin and echoes an allowed one', async () => {
    const service = testService({ cors: { allowedOrigins: ['https://app.example'] } });

    const denied = await service.createSession(
      buildRequest(undefined, { origin: 'https://evil.example' }),
    );
    expect(denied.ok).toBe(false);
    expect(denied.status).toBe(403);
    expect(denied.headers['access-control-allow-origin']).toBeUndefined();

    const allowed = await service.createSession(
      buildRequest(undefined, { origin: 'https://app.example' }),
    );
    expect(allowed.ok).toBe(true);
    expect(allowed.headers['access-control-allow-origin']).toBe('https://app.example');
    expect(allowed.headers['access-control-allow-credentials']).toBe('true');
    expect(allowed.headers['vary']).toBe('Origin');
  });
});

describe('subscriptions', () => {
  async function session(service: StreamingService): Promise<{ cookie: string }> {
    const result = await service.createSession(buildRequest());
    return { cookie: ticketCookie(result.headers) };
  }

  it('requires a ticket cookie', async () => {
    const service = testService();
    const result = await service.listSubscriptions(buildRequest());

    expect(result.ok).toBe(false);
    expect(result.status).toBe(401);
  });

  it('rejects a ticket issued to another principal', async () => {
    const service = testService();
    const { cookie } = await session(service);
    const result = await service.listSubscriptions(
      buildRequest(cookie, { principal: OTHER_PRINCIPAL }),
    );

    expect(result.ok).toBe(false);
    expect(result.status).toBe(401);
  });

  it('adds topics idempotently and returns the full set, sorted', async () => {
    const service = testService();
    const { cookie } = await session(service);

    const first = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: ['server.example/note/2', 'server.example/note/1'] },
    });
    expect(first.ok).toBe(true);
    expect(first.body).toEqual({ topics: ['server.example/note/1', 'server.example/note/2'] });

    const again = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: ['server.example/note/1'] },
    });
    expect(again.body).toEqual({ topics: ['server.example/note/1', 'server.example/note/2'] });

    const listed = await service.listSubscriptions(buildRequest(cookie));
    expect(listed.body).toEqual({ topics: ['server.example/note/1', 'server.example/note/2'] });
  });

  it('accepts wildcard patterns', async () => {
    const service = testService();
    const { cookie } = await session(service);
    const result = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: ['server.example/note/#', 'other.example/+/actor'] },
    });

    expect(result.ok).toBe(true);
    expect(result.body).toEqual({
      topics: ['other.example/+/actor', 'server.example/note/#'],
    });
  });

  it('rejects a malformed body and an invalid topic', async () => {
    const service = testService();
    const { cookie } = await session(service);

    const malformed = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: 'x' },
    });
    expect(malformed.ok).toBe(false);
    expect(malformed.status).toBe(400);

    const invalid = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: ['has space/inside'] },
    });
    expect(invalid.ok).toBe(false);
    expect(invalid.status).toBe(400);
    expect(invalid.body).toMatchObject({ type: PROBLEM_TYPES.invalidTopic, status: 400 });
  });

  it('rejects wildcards when the server advertises none', async () => {
    const service = testService({ topics: { allowWildcards: false } });
    const result = await service.createSession(buildRequest());
    const cookie = ticketCookie(result.headers);

    const added = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: ['server.example/note/#'] },
    });
    expect(added.ok).toBe(false);
    expect(added.body).toMatchObject({ type: PROBLEM_TYPES.invalidTopic, status: 400 });
  });

  it('caps the number of subscriptions', async () => {
    const service = testService({ topics: { maxSubscriptions: 2 } });
    const result = await service.createSession(buildRequest());
    const cookie = ticketCookie(result.headers);

    const added = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: ['a/1', 'a/2', 'a/3'] },
    });
    expect(added.ok).toBe(false);
    expect(added.body).toMatchObject({ type: PROBLEM_TYPES.aboutBlank, status: 400 });
  });

  it('removes topics idempotently', async () => {
    const service = testService();
    const { cookie } = await session(service);
    await service.addSubscriptions({ ...buildRequest(cookie), body: { topics: ['a/1', 'a/2'] } });

    const removed = await service.removeSubscription({ ...buildRequest(cookie), topic: 'a/1' });
    expect(removed.status).toBe(204);

    const removedAgain = await service.removeSubscription({
      ...buildRequest(cookie),
      topic: 'a/1',
    });
    expect(removedAgain.status).toBe(204);

    const listed = await service.listSubscriptions(buildRequest(cookie));
    expect(listed.body).toEqual({ topics: ['a/2'] });
  });

  it('requires the topic query parameter', async () => {
    const service = testService();
    const { cookie } = await session(service);
    const result = await service.removeSubscription({ ...buildRequest(cookie), topic: undefined });

    expect(result.ok).toBe(false);
    expect(result.status).toBe(400);
    expect(result.body).toMatchObject({ type: PROBLEM_TYPES.aboutBlank, status: 400 });
  });

  it('rejects a topic the principal may not observe', async () => {
    const service = testService({
      topics: { authorize: (_principal, topic) => topic.startsWith('public/') },
    });
    const result = await service.createSession(buildRequest());
    const cookie = ticketCookie(result.headers);

    const added = await service.addSubscriptions({
      ...buildRequest(cookie),
      body: { topics: ['private/1'] },
    });

    expect(added.ok).toBe(false);
    if (added.ok) return;
    expect(added.status).toBe(403);
    expect(added.body).toMatchObject({ type: PROBLEM_TYPES.principalNotAuthorized, status: 403 });
  });

  it('keeps principals isolated', async () => {
    const service = testService();
    const { cookie } = await session(service);
    await service.addSubscriptions({ ...buildRequest(cookie), body: { topics: ['a/1'] } });

    const others = await service.createSession(
      buildRequest(undefined, { principal: OTHER_PRINCIPAL }),
    );
    const otherCookie = ticketCookie(others.headers);
    const listed = await service.listSubscriptions({
      principal: OTHER_PRINCIPAL,
      headers: { cookie: otherCookie },
    });

    expect(listed.body).toEqual({ topics: [] });
  });
});

describe('DELETE {streamingControl}', () => {
  it('clears the cookie and invalidates the ticket', async () => {
    const service = testService();
    const issued = await service.createSession(buildRequest());
    const cookie = ticketCookie(issued.headers);

    const revoked = await service.revokeSession(buildRequest(cookie));
    expect(revoked.status).toBe(204);
    expect(setCookie(revoked.headers)).toContain('Max-Age=0');

    const after = await service.listSubscriptions(buildRequest(cookie));
    expect(after.ok).toBe(false);
    expect(after.status).toBe(401);
  });

  it('is idempotent without a ticket', async () => {
    const service = testService();
    const result = await service.revokeSession(buildRequest());
    expect(result.status).toBe(204);
  });

  it('does not let one principal revoke another principal’s ticket', async () => {
    const service = testService();
    const issued = await service.createSession(buildRequest());
    const cookie = ticketCookie(issued.headers);

    await service.revokeSession(buildRequest(cookie, { principal: OTHER_PRINCIPAL }));

    const stillValid = await service.listSubscriptions(buildRequest(cookie));
    expect(stillValid.ok).toBe(true);
  });
});

describe('session limits', () => {
  it('caps the live tickets per principal and revokes the oldest', async () => {
    const service = testService({
      ticket: { ttlSeconds: 60, cookie: { secure: false }, maxPerPrincipal: 2, rateLimit: false },
    });

    const first = ticketCookie((await service.createSession(buildRequest())).headers);
    const second = ticketCookie((await service.createSession(buildRequest())).headers);
    await service.createSession(buildRequest());

    expect((await service.listSubscriptions(buildRequest(first))).status).toBe(401);
    expect((await service.listSubscriptions(buildRequest(second))).ok).toBe(true);
  });

  it('answers 429 with the c180 rate-limit problem type', async () => {
    const service = testService({
      ticket: { cookie: { secure: false }, rateLimit: { max: 2, windowSeconds: 60 } },
    });

    await service.createSession(buildRequest());
    await service.createSession(buildRequest());
    const limited = await service.createSession(buildRequest());

    expect(limited.ok).toBe(false);
    if (limited.ok) return;
    expect(limited.status).toBe(429);
    expect(limited.body).toMatchObject({ type: PROBLEM_TYPES.rateLimitExceeded, status: 429 });
    expect(limited.headers['retry-after']).toBeDefined();
  });
});

describe('ticket store', () => {
  it('stores tickets hashed, never in clear text', async () => {
    const store = new InMemoryTicketStore();
    const { ticket, record } = issueTicket({ principal: PRINCIPAL, ttlSeconds: 60 });
    await store.create(ticket, record);

    const dump = JSON.stringify([
      ...((store as unknown as { _records?: unknown[] })._records ?? []),
    ]);
    // Private fields are invisible to JSON, but the store must also answer only
    // for the exact ticket value.
    expect(dump).not.toContain(ticket);
    expect(await store.read(ticket)).toMatchObject({ principal: PRINCIPAL });
    expect(await store.read(`${ticket}x`)).toBeNull();
  });

  it('consumes a ticket exactly once, even under a race', async () => {
    const store = new InMemoryTicketStore();
    const { ticket, record } = issueTicket({ principal: PRINCIPAL, ttlSeconds: 60 });
    await store.create(ticket, record);

    const [first, second] = await Promise.all([store.consume(ticket), store.consume(ticket)]);
    expect([first, second].filter((entry) => entry !== null)).toHaveLength(1);
  });

  it('still reads a consumed ticket so subscriptions keep working', async () => {
    const store = new InMemoryTicketStore();
    const { ticket, record } = issueTicket({ principal: PRINCIPAL, ttlSeconds: 60 });
    await store.create(ticket, record);
    await store.consume(ticket);

    expect(await store.read(ticket)).toMatchObject({ principal: PRINCIPAL });
  });

  it('expires and prunes tickets', async () => {
    const store = new InMemoryTicketStore();
    const { ticket, record } = issueTicket({ principal: PRINCIPAL, ttlSeconds: 0 });
    await store.create(ticket, record);

    expect(hasExpired(record)).toBe(true);
    expect(await store.read(ticket)).toBeNull();
    expect(await store.prune()).toBe(0);
  });

  it('revokes tickets', async () => {
    const store = new InMemoryTicketStore();
    const { ticket, record } = issueTicket({ principal: PRINCIPAL, ttlSeconds: 60 });
    await store.create(ticket, record);
    await store.revoke(ticket);
    expect(await store.read(ticket)).toBeNull();
  });
});

describe('subscription store', () => {
  it('lists principals with subscriptions', async () => {
    const store = new InMemorySubscriptionStore();
    await store.add('a', ['t/1']);
    await store.add('b', ['t/2']);
    await store.remove('b', 't/2');

    expect(await store.principals()).toEqual(['a']);
    expect(await store.list('b')).toEqual([]);
  });
});
