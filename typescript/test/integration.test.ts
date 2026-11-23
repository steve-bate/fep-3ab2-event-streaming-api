import { afterEach, describe, expect, it } from 'vitest';
import {
  StreamingClient,
  discoverStreamingEndpoint,
  type FetchLike,
} from '@fep-3ab2/streaming/client';
import type { StreamingService } from '@fep-3ab2/streaming/server';
import { createLoopback, waitFor } from '@fep-3ab2/streaming/testing';
import { API_ORIGIN, OTHER_PRINCIPAL, PRINCIPAL, testService } from './helpers.js';

const ACTOR_URI = `${API_ORIGIN}/users/alice`;
const CONTROL_URL = `${API_ORIGIN}/sse/control`;

interface Harness {
  readonly service: StreamingService;
  readonly client: StreamingClient;
  readonly streams: () => number;
  readonly activities: () => unknown[];
  readonly cookieHeader: () => string;
}

function connect(
  service: StreamingService,
  options: {
    readonly principal?: string;
    readonly origin?: string;
    readonly heartbeatTimeoutMs?: number;
    readonly topics?: readonly string[];
  } = {},
): Harness {
  const loopback = createLoopback({
    service,
    principal: options.principal ?? PRINCIPAL,
    ...(options.origin === undefined ? {} : { origin: options.origin }),
  });

  const activities: unknown[] = [];
  const client = new StreamingClient({
    streamingControl: CONTROL_URL,
    fetch: loopback.fetch,
    eventSourceFactory: loopback.eventSourceFactory,
    heartbeatTimeoutMs: options.heartbeatTimeoutMs ?? 0,
    renewBeforeExpiryMs: 0,
    reconnect: { initialDelayMs: 10, maxDelayMs: 50 },
    ...(options.topics === undefined ? {} : { topics: options.topics }),
  });
  client.on('activity', (activity) => activities.push(activity));

  return {
    service,
    client,
    streams: () => loopback.streams.length,
    activities: () => activities,
    cookieHeader: () => loopback.cookieHeader(),
  };
}

const openClients: StreamingClient[] = [];

afterEach(async () => {
  await Promise.all(openClients.splice(0).map((client) => client.close({ revoke: false })));
});

describe('client ↔ server over the loopback', () => {
  it('delivers a published activity to a subscribed client', async () => {
    const service = testService();
    const harness = connect(service, { topics: ['server.example/note/#'] });
    openClients.push(harness.client);

    await harness.client.open();
    await waitFor(() => harness.client.state === 'open');
    await waitFor(() => harness.streams() === 1);

    await service.publish('server.example/note/1', {
      id: 'https://server.example/activities/1',
      type: 'Create',
      object: { type: 'Note', content: 'hello' },
    });

    await waitFor(() => harness.activities().length === 1);
    expect(harness.activities()[0]).toMatchObject({
      id: 'https://server.example/activities/1',
      event: 'activitypub',
      data: { type: 'Create' },
    });
    expect(harness.client.status.topics).toEqual(['server.example/note/#']);
  });

  it('does not deliver topics the client never subscribed to', async () => {
    const service = testService();
    const harness = connect(service, { topics: ['a/#'] });
    openClients.push(harness.client);

    await harness.client.open();
    await waitFor(() => harness.client.state === 'open');

    await service.publish('b/1', { id: 'b-1' });
    await service.publish('a/1', { id: 'a-1' });

    await waitFor(() => harness.activities().length === 1);
    expect(harness.activities()).toHaveLength(1);
    expect(harness.activities()[0]).toMatchObject({ id: 'a-1' });
  });

  it('keeps two principals apart on one server', async () => {
    const service = testService();
    const alice = connect(service, { principal: PRINCIPAL, topics: ['alice/#'] });
    const bob = connect(service, { principal: OTHER_PRINCIPAL, topics: ['bob/#'] });
    openClients.push(alice.client, bob.client);

    await alice.client.open();
    await bob.client.open();
    await waitFor(() => alice.client.state === 'open' && bob.client.state === 'open');

    await service.publish('alice/1', { id: 'alice-1' });
    await service.publish('bob/1', { id: 'bob-1' });

    await waitFor(() => alice.activities().length === 1 && bob.activities().length === 1);
    expect(alice.activities()[0]).toMatchObject({ id: 'alice-1' });
    expect(bob.activities()[0]).toMatchObject({ id: 'bob-1' });
  });

  it('works cross-origin when the origin is allowlisted', async () => {
    const service = testService({ cors: { allowedOrigins: ['https://app.example'] } });
    const harness = connect(service, { origin: 'https://app.example', topics: ['a/1'] });
    openClients.push(harness.client);

    await harness.client.open();
    await waitFor(() => harness.client.state === 'open');

    await service.publish('a/1', { id: 'x' });
    await waitFor(() => harness.activities().length === 1);
    expect(harness.activities()).toHaveLength(1);
  });

  it('refuses a cross-origin client that is not allowlisted', async () => {
    const service = testService({ cors: { allowedOrigins: ['https://app.example'] } });
    const harness = connect(service, { origin: 'https://evil.example' });
    openClients.push(harness.client);

    await expect(harness.client.open()).rejects.toMatchObject({ code: 'http', status: 403 });
  });

  it('revokes the session on close, leaving the cookie dead', async () => {
    const service = testService();
    const harness = connect(service, { topics: ['a/1'] });
    await harness.client.open();
    await waitFor(() => harness.client.state === 'open');

    const cookie = harness.cookieHeader();
    expect(cookie).toContain('sse_ticket=');

    await harness.client.close();

    const after = await service.listSubscriptions({
      principal: PRINCIPAL,
      headers: { cookie },
    });
    expect(after.ok).toBe(false);
    expect(after.status).toBe(401);
  });

  it('reconnects with a new session when the server drops the stream', async () => {
    const service = testService();
    const harness = connect(service, { topics: ['a/1'] });
    openClients.push(harness.client);

    await harness.client.open();
    await waitFor(() => harness.client.state === 'open');

    // Server-side shutdown closes the stream; the client must open a new one.
    service.closeAll();
    await waitFor(() => harness.streams() === 2, { timeoutMs: 3_000 });
    await waitFor(() => harness.client.state === 'open', { timeoutMs: 3_000 });

    await service.publish('a/1', { id: 'after-reconnect' });
    await waitFor(() => harness.activities().length === 1);
    expect(harness.activities()[0]).toMatchObject({ id: 'after-reconnect' });
    // The subscription survived the reconnect.
    expect(harness.client.status.topics).toEqual(['a/1']);
  });

  it('forwards heartbeats to the client', async () => {
    const service = testService({ heartbeatIntervalMs: 100 });
    const harness = connect(service, { heartbeatTimeoutMs: 2_000 });
    openClients.push(harness.client);

    const beats: unknown[] = [];
    harness.client.on('heartbeat', (beat) => beats.push(beat));

    await harness.client.open();
    await waitFor(() => beats.length > 0, { timeoutMs: 2_000 });
    expect(beats[0]).toMatchObject({ receivedAt: expect.any(Date) as Date });
  });

  it('discovers the control endpoint from the actor document and connects', async () => {
    const service = testService();
    const loopback = createLoopback({ service, principal: PRINCIPAL });

    const actorFetch: FetchLike = async (url, init) => {
      if (url === ACTOR_URI) {
        return new Response(
          JSON.stringify({
            '@context': 'https://www.w3.org/ns/activitystreams',
            id: ACTOR_URI,
            type: 'Person',
            endpoints: { streamingControl: CONTROL_URL },
          }),
          { status: 200, headers: { 'content-type': 'activity+json' } },
        );
      }
      return loopback.fetch(url, init);
    };

    const control = await discoverStreamingEndpoint(ACTOR_URI, { fetch: actorFetch });
    expect(control).toBe(CONTROL_URL);

    const client = new StreamingClient({
      streamingControl: control,
      fetch: actorFetch,
      eventSourceFactory: loopback.eventSourceFactory,
      heartbeatTimeoutMs: 0,
      topics: ['a/1'],
    });
    openClients.push(client);

    const received: unknown[] = [];
    client.on('activity', (activity) => received.push(activity));

    await client.open();
    await waitFor(() => client.state === 'open');

    await service.publish('a/1', { id: 'via-discovery' });
    await waitFor(() => received.length === 1);
    expect(received[0]).toMatchObject({ id: 'via-discovery' });
  });
});
