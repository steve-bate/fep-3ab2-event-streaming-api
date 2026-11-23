import { afterEach, describe, expect, it, vi } from 'vitest';
import {
  StreamingClient,
  StreamingClientError,
  discoverStreamingEndpoint,
  type FetchLike,
} from '@fep-3ab2/streaming/client';
import { createEventSourceRecorder } from '@fep-3ab2/streaming/testing';

const CONTROL_URL = 'https://api.example/sse/control';
const SUBSCRIPTIONS_URL = 'https://api.example/sse/control/subscriptions';

interface Call {
  readonly url: string;
  readonly method: string;
  readonly body: string | undefined;
}

/** A canned FEP-3ab2 server: just enough of one to drive the client. */
function createCannedServer(
  options: {
    readonly ttlSeconds?: number;
    readonly topics?: string[];
    readonly heartbeatInterval?: number;
  } = {},
) {
  const calls: Call[] = [];
  const topics = new Set(options.topics ?? []);
  let sessionCount = 0;
  let failure: Response | undefined;

  const fetchImpl: FetchLike = async (url, init) => {
    const method = init.method ?? 'GET';
    calls.push({
      url,
      method,
      body: typeof init.body === 'string' ? init.body : undefined,
    });

    if (failure !== undefined) {
      const response = failure;
      failure = undefined;
      return response;
    }

    if (url === CONTROL_URL) {
      if (method === 'POST') {
        sessionCount += 1;
        return json(201, {
          subscriptions_url: SUBSCRIPTIONS_URL,
          stream_url: `https://api.example/sse/stream?session=${sessionCount}`,
          expires_at: new Date(Date.now() + (options.ttlSeconds ?? 300) * 1_000).toISOString(),
          ...(options.heartbeatInterval === undefined
            ? {}
            : { heartbeat_interval: options.heartbeatInterval }),
          wildcard_support: true,
        });
      }
      if (method === 'DELETE') return new Response(null, { status: 204 });
    }

    if (url.startsWith(SUBSCRIPTIONS_URL)) {
      if (method === 'POST') {
        const payload = JSON.parse(typeof init.body === 'string' ? init.body : '{}') as {
          topics: string[];
        };
        for (const topic of payload.topics) topics.add(topic);
        return json(200, { topics: [...topics].sort() });
      }
      if (method === 'GET') return json(200, { topics: [...topics].sort() });
      if (method === 'DELETE') {
        const target = new URL(url);
        topics.delete(target.searchParams.get('topic') ?? '');
        return new Response(null, { status: 204 });
      }
    }

    return new Response('not found', { status: 404 });
  };

  return {
    fetch: fetchImpl,
    calls,
    get sessionCount(): number {
      return sessionCount;
    },
    get topics(): string[] {
      return [...topics].sort();
    },
    failNextWith(response: Response): void {
      failure = response;
    },
  };
}

function json(status: number, body: unknown, mediaType = 'application/json'): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': mediaType },
  });
}

function createClient(server: { fetch: FetchLike }, recorder = createEventSourceRecorder()) {
  const client = new StreamingClient({
    streamingControl: CONTROL_URL,
    fetch: server.fetch,
    eventSourceFactory: recorder.factory,
    heartbeatTimeoutMs: 0,
  });
  return { client, recorder };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('StreamingClient', () => {
  it('rejects a relative streaming control URL', () => {
    expect(() => new StreamingClient({ streamingControl: '/sse/control' })).toThrow(
      StreamingClientError,
    );
  });

  it('opens a session and a stream', async () => {
    const server = createCannedServer();
    const { client, recorder } = createClient(server);
    const states: string[] = [];
    client.on('state', (state) => states.push(state));

    const session = await client.open();

    expect(server.calls[0]).toMatchObject({ url: CONTROL_URL, method: 'POST' });
    expect(session.streamUrl).toBe('https://api.example/sse/stream?session=1');
    expect(session.wildcardSupport).toBe(true);
    expect(recorder.instances).toHaveLength(1);
    expect(recorder.instances[0]?.url).toBe(session.streamUrl);
    expect(recorder.instances[0]?.withCredentials).toBe(true);

    recorder.instances[0]?.open();
    expect(client.state).toBe('open');
    expect(states).toEqual(['connecting', 'open']);
    await client.close({ revoke: false });
  });

  it('applies constructor topics once the session exists', async () => {
    const server = createCannedServer({ topics: ['existing/1'] });
    const recorder = createEventSourceRecorder();
    const client = new StreamingClient({
      streamingControl: CONTROL_URL,
      fetch: server.fetch,
      eventSourceFactory: recorder.factory,
      heartbeatTimeoutMs: 0,
      topics: ['server.example/note/#'],
    });
    const seen: string[][] = [];
    client.on('subscriptions', (topics) => seen.push([...topics]));

    await client.open();

    expect(server.topics).toEqual(['existing/1', 'server.example/note/#']);
    expect(seen.at(-1)).toEqual(['existing/1', 'server.example/note/#']);
    await client.close({ revoke: false });
  });

  it('re-applies its topic mirror on reconnect', async () => {
    vi.useFakeTimers();
    const server = createCannedServer();
    const { client, recorder } = createClient(server);

    await client.open();
    await client.subscribe(['a/1']);
    recorder.instances[0]?.open();

    recorder.instances[0]?.fail();
    await vi.advanceTimersByTimeAsync(1_000);

    const subscriptionBodies = server.calls
      .filter((call) => call.method === 'POST' && call.url.startsWith(SUBSCRIPTIONS_URL))
      .map((call) => JSON.parse(call.body ?? '{}') as { topics: string[] });

    expect(subscriptionBodies).toEqual([{ topics: ['a/1'] }, { topics: ['a/1'] }]);
    await client.close({ revoke: false });
  });

  it('dispatches ActivityPub frames with their id and payload', async () => {
    const server = createCannedServer();
    const { client, recorder } = createClient(server);
    const received: unknown[] = [];
    client.on('activity', (activity) => received.push(activity));

    await client.open();
    const source = recorder.instances[0];
    source?.open();
    source?.emit('activitypub', { id: 'https://x/1', type: 'Create' }, { id: 'https://x/1' });

    expect(received).toEqual([
      {
        id: 'https://x/1',
        event: 'activitypub',
        data: { id: 'https://x/1', type: 'Create' },
        receivedAt: expect.any(Date) as Date,
      },
    ]);
    await client.close({ revoke: false });
  });

  it('derives the watchdog from the advertised heartbeat_interval', async () => {
    vi.useFakeTimers();
    const server = createCannedServer({ heartbeatInterval: 5 });
    const recorder = createEventSourceRecorder();
    const client = new StreamingClient({
      streamingControl: CONTROL_URL,
      fetch: server.fetch,
      eventSourceFactory: recorder.factory,
      reconnect: { maxAttempts: 0 },
    });
    const errors: Error[] = [];
    client.on('error', (error) => errors.push(error));

    await client.open();
    recorder.instances[0]?.open();

    // Twice the advertised interval: 10 s of silence is still healthy …
    await vi.advanceTimersByTimeAsync(9_000);
    expect(errors).toHaveLength(0);

    // … and a moment past it is not.
    await vi.advanceTimersByTimeAsync(1_100);
    expect(errors[0]?.message).toMatch(/within 10000 ms/);

    await client.close({ revoke: false });
  });

  it('dispatches heartbeats', async () => {
    const server = createCannedServer();
    const { client, recorder } = createClient(server);
    const beats: unknown[] = [];
    client.on('heartbeat', (beat) => beats.push(beat));

    await client.open();
    recorder.instances[0]?.open();
    recorder.instances[0]?.emit('heartbeat', { ts: '2026-03-10T12:00:00Z' });

    expect(beats).toEqual([
      { receivedAt: expect.any(Date) as Date, data: { ts: '2026-03-10T12:00:00Z' } },
    ]);
    await client.close({ revoke: false });
  });

  it('manages subscriptions through the session URLs', async () => {
    const server = createCannedServer();
    const { client, recorder } = createClient(server);
    await client.open();
    recorder.instances[0]?.open();

    expect(await client.subscribe('a/1')).toEqual(['a/1']);
    expect(await client.subscribe(['a/2', 'b/#'])).toEqual(['a/1', 'a/2', 'b/#']);
    expect(await client.refreshSubscriptions()).toEqual(['a/1', 'a/2', 'b/#']);
    expect(await client.unsubscribe('a/1')).toEqual(['a/2', 'b/#']);

    const removal = server.calls.find(
      (call) => call.method === 'DELETE' && call.url.includes('topic='),
    );
    expect(removal?.url).toContain('topic=a%2F1');
    await client.close({ revoke: false });
  });

  it('refuses subscription calls before open()', async () => {
    const server = createCannedServer();
    const { client } = createClient(server);

    await expect(client.subscribe('a/1')).rejects.toThrow(/open\(\)/);
    await expect(client.unsubscribe('a/1')).rejects.toThrow(/open\(\)/);
  });

  it('revokes the session and disconnects on close()', async () => {
    const server = createCannedServer();
    const { client, recorder } = createClient(server);
    await client.open();
    recorder.instances[0]?.open();

    await client.close();

    const revoke = server.calls.find(
      (call) => call.url === CONTROL_URL && call.method === 'DELETE',
    );
    expect(revoke).toBeDefined();
    expect(recorder.instances[0]?.readyState).toBe('closed');
    expect(client.state).toBe('closed');
    await expect(client.open()).rejects.toThrow(/closed/);
  });

  it('reconnects with a fresh ticket after a stream error', async () => {
    vi.useFakeTimers();
    const server = createCannedServer();
    const { client, recorder } = createClient(server);

    await client.open();
    recorder.instances[0]?.open();
    recorder.instances[0]?.fail();
    await vi.advanceTimersByTimeAsync(1_000);

    // A spent ticket cannot be reused, so the client must ask for a new session.
    expect(server.sessionCount).toBe(2);
    expect(recorder.instances).toHaveLength(2);
    expect(recorder.instances[0]?.readyState).toBe('closed');
    await client.close({ revoke: false });
  });

  it('renews the ticket before it expires', async () => {
    vi.useFakeTimers();
    const server = createCannedServer({ ttlSeconds: 10 });
    const recorder = createEventSourceRecorder();
    const client = new StreamingClient({
      streamingControl: CONTROL_URL,
      fetch: server.fetch,
      eventSourceFactory: recorder.factory,
      heartbeatTimeoutMs: 0,
      renewBeforeExpiryMs: 1_000,
    });

    await client.open();
    recorder.instances[0]?.open();

    await vi.advanceTimersByTimeAsync(9_200);

    expect(server.sessionCount).toBe(2);
    expect(recorder.instances).toHaveLength(2);
    await client.close({ revoke: false });
  });

  it('treats a silent stream as dead', async () => {
    vi.useFakeTimers();
    const server = createCannedServer();
    const recorder = createEventSourceRecorder();
    const client = new StreamingClient({
      streamingControl: CONTROL_URL,
      fetch: server.fetch,
      eventSourceFactory: recorder.factory,
      heartbeatTimeoutMs: 1_000,
    });

    await client.open();
    recorder.instances[0]?.open();

    const errors: Error[] = [];
    client.on('error', (error) => errors.push(error));

    await vi.advanceTimersByTimeAsync(1_200);
    expect(errors[0]?.message).toMatch(/heartbeat/);

    await vi.advanceTimersByTimeAsync(1_000);
    expect(recorder.instances).toHaveLength(2);
    await client.close({ revoke: false });
  });

  it('stops after the configured number of attempts', async () => {
    vi.useFakeTimers();
    const server = createCannedServer();
    const recorder = createEventSourceRecorder();
    const client = new StreamingClient({
      streamingControl: CONTROL_URL,
      fetch: server.fetch,
      eventSourceFactory: recorder.factory,
      heartbeatTimeoutMs: 0,
      reconnect: { maxAttempts: 1, initialDelayMs: 10 },
    });

    const errors: Error[] = [];
    client.on('error', (error) => errors.push(error));

    await client.open();
    recorder.instances[0]?.open();

    // The one permitted retry fails too, so the client gives up.
    server.failNextWith(new Response('boom', { status: 500 }));
    recorder.instances[0]?.fail();
    await vi.advanceTimersByTimeAsync(100);

    expect(errors.at(-1)?.message).toMatch(/exhausted/);
    expect(client.state).toBe('closed');
    expect(
      server.calls.filter((call) => call.url === CONTROL_URL && call.method === 'POST'),
    ).toHaveLength(2);
  });

  it('reports HTTP failures with status and the problem detail', async () => {
    const server = createCannedServer();
    server.failNextWith(
      json(
        401,
        {
          type: 'https://w3id.org/fep/3ab2#invalid-ticket',
          title: 'Invalid ticket',
          status: 401,
          detail: 'Not signed in',
        },
        'application/problem+json',
      ),
    );
    const { client } = createClient(server);

    await expect(client.open()).rejects.toMatchObject({
      name: 'StreamingClientError',
      code: 'http',
      status: 401,
      message: 'Invalid ticket: Not signed in',
      problem: { type: 'https://w3id.org/fep/3ab2#invalid-ticket', status: 401 },
    });
  });

  it('rejects a session response that is missing stream_url', async () => {
    const server = createCannedServer();
    server.failNextWith(
      json(201, {
        subscriptions_url: SUBSCRIPTIONS_URL,
        expires_at: new Date().toISOString(),
        wildcard_support: true,
      }),
    );
    const { client } = createClient(server);

    await expect(client.open()).rejects.toMatchObject({ code: 'protocol' });
  });
});

describe('discoverStreamingEndpoint', () => {
  it('reads endpoints.streamingControl from an actor document', async () => {
    const fetchImpl: FetchLike = async () =>
      json(200, {
        '@context': 'https://www.w3.org/ns/activitystreams',
        id: 'https://api.example/users/alice',
        type: 'Person',
        endpoints: { streamingControl: CONTROL_URL },
      });

    await expect(
      discoverStreamingEndpoint('https://api.example/users/alice', { fetch: fetchImpl }),
    ).resolves.toBe(CONTROL_URL);
  });

  it('accepts the JSON-LD object form', async () => {
    const fetchImpl: FetchLike = async () =>
      json(200, { endpoints: { streamingControl: { '@id': CONTROL_URL } } });

    await expect(
      discoverStreamingEndpoint('https://api.example/users/alice', { fetch: fetchImpl }),
    ).resolves.toBe(CONTROL_URL);
  });

  it('fails when the actor advertises no streaming endpoint', async () => {
    const fetchImpl: FetchLike = async () => json(200, { id: 'https://api.example/users/alice' });

    await expect(
      discoverStreamingEndpoint('https://api.example/users/alice', { fetch: fetchImpl }),
    ).rejects.toMatchObject({ code: 'protocol' });
  });
});
