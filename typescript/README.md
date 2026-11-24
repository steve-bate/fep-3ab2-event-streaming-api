# FEP-3ab2 streaming: TypeScript client and server toolkit

A reference implementation of [FEP-3ab2: ActivityPub Event Streaming API](../../fep-3ab2.md) for
TypeScript, plus an example server and demo page that exercise every part of the protocol. including
the cross-origin path the FEP spends a whole section on.

Zero runtime dependencies. The library talks to `fetch`, `EventSource` and WebCrypto, so the same
code runs in a browser, in Node, and in an edge worker.

```
topics ── URI → topic derivation, MQTT-style patterns, validation
client ── StreamingClient: session, stream, renewal, reconnect, subscriptions
server ── createStreamingService: control API + SSE endpoint as plain functions
testing ─ in-process loopback, fake EventSource, SSE parser
```

## Requirements

Node 22+ (for the tooling and the example). The library itself has no Node dependencies.

## Layout

| Entry point                   | Contents                                                  |
| ----------------------------- | --------------------------------------------------------- |
| `@fep-3ab2/streaming`         | Alias of `/topics` (the shared, safe-to-import module)    |
| `@fep-3ab2/streaming/topics`  | `topicFromUri`, `validateTopic`, `compileTopicPattern`, … |
| `@fep-3ab2/streaming/client`  | `StreamingClient`, session helpers, discovery             |
| `@fep-3ab2/streaming/server`  | `createStreamingService`, stores, event hub, SSE framing  |
| `@fep-3ab2/streaming/testing` | `createLoopback`, `createFakeEventSource`, `SseParser`    |

The client subpath never imports the server and vice versa. Both import `topics` and the shared wire
types in `src/protocol.ts`, so a rename on one side is a compile error on the other.

## Client

```ts
import { StreamingClient, discoverStreamingEndpoint } from '@fep-3ab2/streaming/client';

// 1. Discovery: read endpoints.streamingControl from the actor document.
const streamingControl = await discoverStreamingEndpoint('https://server.example/users/alice');

// 2. One client, one SSE connection, any number of topics.
const client = new StreamingClient({ streamingControl, topics: ['notifications'] });

client.on('activity', (activity) => {
  console.log(activity.id, activity.data); // activity.data is the ActivityPub entity
});
client.on('state', (state) => console.log('stream state:', state));
client.on('heartbeat', () => console.log('still alive'));

await client.open(); // POST /control, then GET stream_url
await client.subscribe('server.example/note/#');
await client.unsubscribe('server.example/note/#');
await client.close(); // closes the stream and revokes the ticket
```

What the client handles for you, because the FEP requires it:

- **Ticket cookies, not credentials in URLs.** `credentials: 'include'` on `fetch` and
  `withCredentials: true` on `EventSource`, so the ticket travels in an `HttpOnly` cookie.
- **Single-use tickets.** The browser's automatic `EventSource` retry is switched off: it would
  replay a spent ticket and get a `401`. The client closes the source and reconnects with a _new_
  session, with exponential backoff and jitter.
- **Renewal before expiry.** A timer fires at `expires_at - renewBeforeExpiryMs` (default 5 s) and
  opens a fresh session and stream before the old ticket lapses.
- **Self-healing subscriptions.** The topic mirror is re-applied to every new session (subscriptions
  belong to the principal, and re-adding is idempotent), so a server restart does not silently leave
  you subscribed to nothing.
- **Dead-stream detection.** When the server advertises `heartbeat_interval`, silence for twice that
  long is treated as a dropped connection: the FEP's own failure detector. `heartbeatTimeoutMs`
  overrides it; with no advertised interval there is no watchdog and silence surfaces only as a
  transport error.
- **Problem details.** A control-API failure throws `StreamingClientError` carrying `status`, `body`
  and `problem: { type, title, status, detail }`, so callers branch on the RFC 9457 `type` rather
  than on a message string.

Options worth knowing: `fetch`, `eventSourceFactory`, `credentials`, `withCredentials`, `headers`
(e.g. `authorization` for token-auth servers), `heartbeatTimeoutMs`, `renewBeforeExpiryMs`,
`reconnect: { initialDelayMs, maxDelayMs, maxAttempts, factor, jitter }`, `topics`, `logger`.

Session calls are also available as standalone functions (`createStreamingSession`,
`addSubscriptions`, `listSubscriptions`, `removeSubscription`, `revokeStreamingSession`) for code
that only needs the control API: a bot, a CLI, a test.

## Server

The server subpath is framework-agnostic: a request comes in as
`{ principal, headers, origin, signal }` and the answer is a status, headers and a body, or, for the
stream, an async source of wire-ready SSE chunks.

```ts
import { createStreamingService } from '@fep-3ab2/streaming/server';

const service = createStreamingService({
  publicOrigin: 'https://api.example', // absolute URLs in the session response
  ticket: { ttlSeconds: 300, cookie: { name: 'sse_ticket', path: '/sse', secure: true } },
  heartbeatIntervalMs: 20_000,
  cors: { allowedOrigins: ['https://app.example'] },
});

// POST /sse/control
app.post('/sse/control', (req, res) => {
  const result = await service.createSession({
    principal: req.user.actorUri, // your auth, your identity
    headers: req.headers,
    origin: req.headers.origin,
  });
  res.status(result.status).set(result.headers).json(result.body);
});

// GET /sse/stream
app.get('/sse/stream', async (req, res) => {
  const controller = new AbortController();
  req.on('close', () => controller.abort());

  const result = await service.openStream({
    principal: req.user.actorUri,
    headers: req.headers,
    origin: req.headers.origin,
    signal: controller.signal,
  });
  if (!result.ok) {
    res.status(result.status).set(result.headers).json(result.body);
    return;
  }

  res.writeHead(result.status, result.headers);
  for await (const chunk of result.frames) res.write(chunk);
  res.end();
});

// Publishing is what an application does: any ActivityPub entity, any concrete topic.
await service.publish('server.example/note/1', createActivity);
```

Methods: `createSession`, `revokeSession`, `listSubscriptions`, `addSubscriptions`,
`removeSubscription`, `openStream`, `preflight` (OPTIONS), `cors` (per-request headers), `publish`,
`pruneExpiredTickets`, `closeAll`.

Everything is injectable: `ticketStore` and `subscriptionStore` (both have in-memory implementations
and interfaces a database backend can satisfy), `logger`, `urls`,
`topics: { allowWildcards, maxLength, maxSubscriptions, authorize }`,
`ticket: { ttlSeconds, cookie, maxPerPrincipal, rateLimit }`, `queueLimit`, and `publicOrigin`.

`topics.authorize(principal, topic)` is what keeps a server with private topics conformant: a
`POST {subscriptions_url}` naming a topic it rejects is answered `403` with the FEP-c180
`principal-not-authorized` type, instead of being accepted and delivering nothing.

### Errors

Every failure is an RFC 9457 problem detail served as `application/problem+json`.
`problem(type, status, detail)` builds one, `PROBLEM_TYPES` carries the type URIs:

| Status | `type`                                               | When                                                    |
| ------ | ---------------------------------------------------- | ------------------------------------------------------- |
| 400    | `https://w3id.org/fep/3ab2#invalid-topic`            | topic structure, or unsupported wildcard                |
| 400    | `about:blank`                                        | malformed body, missing `topic` query                   |
| 401    | `https://w3id.org/fep/3ab2#invalid-ticket`           | missing, expired, used, other principal                 |
| 403    | `https://w3id.org/fep/3ab2#origin-not-allowed`       | untrusted `Origin`, or one the ticket was not issued to |
| 403    | `https://w3id.org/fep/c180#principal-not-authorized` | topic the principal may not observe                     |
| 405    | `about:blank`                                        | method the endpoint does not define                     |
| 406    | `about:blank`                                        | `Accept` does not name the stream type                  |
| 415    | `about:blank`                                        | subscription body is not `application/json`             |
| 429    | `https://w3id.org/fep/c180#rate-limit-exceeded`      | session issuance over the limit                         |

### What it enforces

| Requirement (FEP-3ab2)                                                     | Where                                  |
| -------------------------------------------------------------------------- | -------------------------------------- |
| 256-bit random ticket, unguessable                                         | `generateTicket()`, `crypto.ts`        |
| Ticket values never stored or logged in clear text                         | `InMemoryTicketStore` keys on SHA-256  |
| Ticket is single-use; one stream per ticket                                | `TicketStore.consume()` (race-guarded) |
| Ticket must match the authenticated principal                              | every ticket-guarded call              |
| One stream per principal (a second supersedes the first)                   | `openStream`                           |
| Stream ends on ticket expiry, revocation, disconnect                       | `openStream` + `revokeSession`         |
| `#` matches zero or more segments, `+` exactly one                         | `compileTopicPattern`                  |
| Wildcards must fill a segment; literal `+`/`#` escaped                     | `validateTopic`                        |
| Adding/removing subscriptions is idempotent                                | `SubscriptionStore`                    |
| Absolute `subscriptions_url` / `stream_url` in the session                 | `createSession`                        |
| `heartbeat_interval` in the 201 whenever heartbeats are on                 | `createSession`                        |
| Strict CORS: exact origin, credentials, `Vary: Origin`, `403` when refused | `evaluateCors`, `preflightHeaders`     |
| Client-bound origin trusted through `originTrust`                          | `corsHeaders`, `preflight`             |
| Stream `Origin` must match the ticket's recorded origin                    | `openStream`                           |
| `415` unless the subscription body is `application/json`                   | `addSubscriptions`                     |
| Heartbeats every 15–30 s (default 20 s)                                    | `openStream` frame loop                |
| `406` unless `Accept` names `text/event-stream`                            | `openStream`                           |
| Superseded stream's ticket is revoked with the stream                      | `openStream`                           |
| Live tickets capped per principal, oldest revoked                          | `createSession`                        |
| `429` with the c180 type over the session rate limit                       | `createSession`                        |
| Errors as RFC 9457 problem details                                         | `problem()`, every failure path        |

Subscriptions are kept per principal, so a consumed (already-used) ticket still authorises
subscription management; only opening a second stream is refused, as the spec requires.

## Running the demo

```bash
npm install
npm run example
```

- demo page: <http://localhost:5180>
- FEP-3ab2 API: <http://localhost:5181>
- credentials: `alice` / `demo`

The page and the API are on **different origins**, so the demo only works because the CORS rules in
the FEP are implemented: exact `Access-Control-Allow-Origin`, credentialed requests, `Vary: Origin`,
preflight for the `POST`s and the `DELETE …?topic=`, and an allowlist that answers `403` for
anything else.

**Client-bound origins.** `DEMO_WEB_ORIGIN` is the allowlist, and it is not the only way in. The
demo's `/demo/token` stands in for an OAuth 2.0 token endpoint: the signed-in Principal gets a
bearer token that names the client and the origin it registered (`DEMO_CLIENTS`). The service takes
those two answers in turn — `cors.allowedOrigins` first, then the `originTrust` hook — so a client
on a domain the server has never heard of gets served without a configuration change. The ticket
records the origin it was issued to, and `openStream` refuses a stream whose `Origin` differs, which
is what keeps the cookie-only stream request as narrow as an allowlist would.

Sign in (Basic once, then a session cookie, because `EventSource` cannot send headers), connect,
type something and press **Create Note**. The page POSTs a `Create{Note}` to `/users/alice/inbox`;
the server does what inbox processing does: it publishes the received activity to the inbox topic
(`localhost:5180/users/alice/inbox`, derived by `topicFromUri`), and the open stream delivers it
back.

Two panels show the same traffic from different angles: **Stream** is the raw JSON of every frame,
and **Timeline** is a view rebuilt _only_ from those events, where **Update** posts an
`Update{Note}` and **Delete** posts a `Delete` with a `Tombstone`. The client keeps no note state of
its own, so the timeline is a direct readout of what the stream delivered. Nothing else emits
events: the stream stays silent until you post. Ticket TTL is 120 s in the demo, so a long enough
session shows renewal.

Ports, origins and credentials come from `typescript/.env` (see [Configuration](#configuration)),
and the same keys work as environment variables.

Either loopback spelling works: <http://localhost:5180> and <http://127.0.0.1:5180> are different
origins to a browser, so the demo allowlists both, the page calls the API on the host name it was
loaded from, and the service derives `subscriptions_url` / `stream_url` from the request's `Host`
header instead of a hard-coded `publicOrigin`.

**One port only (port-forwarding, remote access).** The default demo is cross-origin, so it needs
_two_ reachable ports: a browser that can only reach 5180 will call `localhost:5181` and find
nothing there. Either forward both ports, or mount the API on the page's origin:

```bash
DEMO_SINGLE_ORIGIN=1 npm run example     # page and API both on :5180
```

In that mode the page uses relative URLs, the actor document is served from the same origin, and
`DEMO_API_PORT` is not used.

If sign-in fails, the page says why. `cannot reach http://…/demo/login` means the request never
arrived; nothing is listening on that port (or, if you forwarded only the page's port, the API is
not forwarded; use `DEMO_SINGLE_ORIGIN=1`). A `403` in the log is the CORS allowlist refusing the
page's origin, and a `401` is the credentials: the page prints which username it sent and reminds
you that a password field can be overwritten by browser autofill.

The example is small but not fake: it has an actor document with `endpoints.streamingControl`, a
session cookie, the control API, the SSE endpoint, a real inbox that feeds the stream, and a browser
client that discovers the endpoint rather than hard-coding it.

## Configuration

Copy `.env.example` to `.env` (it is gitignored; real environment variables win over it). Every key
has a working default, so an empty file runs the loopback demo unchanged.

| Key                               | Default                        | Meaning                                                                                                         |
| --------------------------------- | ------------------------------ | --------------------------------------------------------------------------------------------------------------- |
| `DEMO_API_ORIGIN`                 | unset                          | absolute origin the API advertises, e.g. `https://api.example.com`; unset derives it from each request's `Host` |
| `DEMO_WEB_ORIGIN`                 | unset                          | absolute origin the page is served from; added to the CORS allowlist                                            |
| `DEMO_WEB_ORIGINS`                | unset                          | comma-separated allowlist, overriding `DEMO_WEB_ORIGIN`                                                         |
| `DEMO_API_PORT` / `DEMO_WEB_PORT` | 5181 / 5180                    | listen ports                                                                                                    |
| `DEMO_SINGLE_ORIGIN`              | unset                          | `1` mounts the API on the page's origin (one port)                                                              |
| `DEMO_TICKET_TTL_SECONDS`         | 120                            | ticket lifetime                                                                                                 |
| `DEMO_HEARTBEAT_INTERVAL_MS`      | 10000                          | heartbeat interval; advertised rounded up to whole seconds                                                      |
| `DEMO_COOKIE_SAMESITE`            | `Lax`                          | ticket cookie `SameSite`; `None` needs an https API origin                                                      |
| `DEMO_CLIENTS`                    | derived from `DEMO_WEB_ORIGIN` | `id@origin` pairs that may request a token, each with the origin it registered                                  |
| `DEMO_USER` / `DEMO_PASSWORD`     | alice / demo                   | demo credentials                                                                                                |
| `DEMO_LOG`                        | 1                              | `0` silences the per-request streaming log                                                                      |
| `DEMO_ENV_FILE`                   | `typescript/.env`              | read a different env file                                                                                       |

Setting `DEMO_API_ORIGIN` and `DEMO_WEB_ORIGIN` is what makes the demo usable behind a proxy or on a
real host name: the session response advertises those URLs, the CORS allowlist names that page
origin, and the ticket cookie gets `Secure` because the API origin is `https`.

### Cross-site testing

Two origins on the _same_ registrable domain (a page on `app.example.com` calling an API on
`api.example.com`) are same-site, so the default `SameSite=Lax` cookie is sent and nothing else is
needed.

A page and an API on _different_ registrable domains are cross-site, and `Lax` is not sent on the
`EventSource` request; the stream would answer `401 invalid-ticket` with a valid cookie in hand.
That pair needs `SameSite=None; Secure`, so the API origin must be https:

```ini
DEMO_API_ORIGIN=https://api.example.com
DEMO_WEB_ORIGIN=https://app.example.net
DEMO_COOKIE_SAMESITE=None
```

The config loader refuses `None` without an https API origin rather than letting the browser drop
the cookie silently. Note that browsers with third-party cookies blocked (Safari by default, Chrome
under its 3PC restrictions) will withhold the cookie even then, which is exactly the case the FEP
leaves open between the cookie profile and a header-carried ticket.

## Testing

```bash
npm test            # unit + integration tests
npm run coverage
npm run lint        # eslint 10, typescript-eslint strictTypeChecked
npm run typecheck   # src + tests
npm run typecheck:examples
npm run check       # typecheck + lint + test
```

`@fep-3ab2/streaming/testing` is what makes the client and server testable without sockets:
`createLoopback({ service, principal })` returns a `fetch` and an `EventSourceFactory` wired
straight to a real `StreamingService`, with a cookie jar in place of the browser. Two loopbacks
against one service is how the isolation tests are written.

```ts
const service = createStreamingService({ publicOrigin: 'https://api.example' });
const loopback = createLoopback({ service, principal: 'https://api.example/users/alice' });
const client = new StreamingClient({
  streamingControl: 'https://api.example/sse/control',
  fetch: loopback.fetch,
  eventSourceFactory: loopback.eventSourceFactory,
});
```

`createFakeEventSource()` / `createEventSourceRecorder()` cover the client alone, and `SseParser`
parses the exact bytes the server produced.

## Notes and limits

- **Ticket storage is in memory.** `TicketStore` is an interface; a multi-process deployment must
  back it with something shared and make `consume()` atomic there too.
- **Subscriptions are in memory**, likewise behind `SubscriptionStore`.
- **The event hub is per process.** A publication only reaches streams held by the same process;
  behind a load balancer you need a shared bus (Redis, NATS) feeding each node's `publish`.
- **Delivery is best-effort within `queueLimit`** (default 256 frames per stream). A consumer that
  falls behind loses the oldest frames rather than growing memory without bound; the drop count is
  logged when the stream closes.
- **No replay.** `Last-Event-ID` is sent by browsers but the FEP defines no replay semantics, so a
  reconnect is a fresh subscription, not a resume.
- **The demo's authentication is not authentication.** A `demo_session` cookie holding a username
  exists only so the page knows `EventSource` sends no headers.

## License

CC0 1.0 Universal, the same public-domain dedication as the FEP itself.
