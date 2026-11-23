---
slug: "3ab2"
type: implementation
authors: Steve Bate <svc-fep@stevebate.net>
status: DRAFT
discussionsTo: https://github.com/steve-bate/fep-3ab2-event-streaming-api/issues
---

# FEP-3ab2: ActivityPub Event Streaming API

## Motivation

The ActivityPub specification defines a server-to-server federation protocol but does not specify how a *client* application (e.g. a web front-end or mobile app) receives real-time notifications from an ActivityPub server. This FEP proposes a straightforward, standards-aligned approach using [HTTP/1.1 Server-Sent Events][SSE].

Goals:
- Reuse existing HTTP authentication.
- Efficiently use SSE streaming resources.
- Provide a discoverable, RESTful control API for managing SSE sessions.

This proposal addresses two issues related to SSE event streaming in an ActivityPub context.

* **Credential leaks**: Since the SSE browser `EventSource` API does not allow sending HTTP headers when opening an SSE event stream, some proposals recommend sending credentials in the streaming URL. However, this is not desirable because of the risk that the credentials may be exposed in server logs (including a proxy server outside the control of the AP server operator).
* **SSE Concurrent Connection Limits**: Browsers enforce a small per-origin limit on concurrent HTTP connections (commonly 6), and each SSE stream occupies one of those connections for as long as it stays open, which can block other requests to the same origin. This proposal multiplexes multiple subscriptions on a single SSE connection rather than creating a stream per subscription.

## Summary

This FEP specifies a lightweight, cookie-authenticated [**Server-Sent Events (SSE)**][SSE] streaming API that ActivityPub server implementations can expose to their authenticated clients. The API provides:

1. A **session-control sub-API** for issuing and revoking short-lived, single-use streaming tickets, and for managing per-Principal topic subscriptions.
2. A **stream endpoint** that delivers a multiplexed, real-time event feed for all topics to which the authenticated Principal is subscribed.

The design intentionally separates *authentication* (handled by the server's existing mechanism, e.g. [OAuth 2.0][OAUTH2], session cookies, or HTTP Basic) from *streaming authorization* (a short-lived ticket stored in an `HttpOnly` cookie), so that the SSE connection never carries user credentials.

## Terminology

| Term | Definition |
|------|-----------|
| **Actor** | A standard [ActivityPub] actor |
| **Principal** | The authenticated identity (URI of the Actor) making a request. |
| **Ticket** | A short-lived, randomly generated opaque token that authorizes one SSE stream connection. |
| **Topic** | A string identifying a channel of events (e.g. `notifications`, `inbox`, `timeline:home`). |
| **SSE Session** | The lifecycle from ticket issuance through stream connection to ticket revocation or expiry. |

The key words "MUST", "MUST NOT", "REQUIRED", "SHALL", "SHALL NOT", "SHOULD", "SHOULD NOT", "RECOMMENDED", "MAY", and "OPTIONAL" in this specification are to be interpreted as described in [RFC 2119][RFC-2119].


## Endpoint Discovery

The streaming control endpoint is exposed via the [ActivityPub] Actor `endpoints` property, as the `streamingControl` endpoint property.

```json
{
  "@context": [
    "https://w3id.org/fep/3ab2",
    "https://www.w3.org/ns/activitystreams"
  ],
  "type": "Person",
  "id": "https://server.example/actor/1",
  ...
  "endpoints": {
    "streamingControl": "https://server.example/streaming/control"
  }
}
```

## Topics

A topic is one or more text segments separated by the `/` character.

### Topic Wildcards
For subscriptions, topics MAY contain wildcards if the server supports them. The standard [MQTT wildcards][MQTT Wildcards] are used for this purpose. A server can advertise wildcard support in the [`Create Session`](#create-session--post-streamingcontrol) response via the `wildcard_support` field.

| Wildcard | Description | Example |
|-----|------|------|
| + | Matches a single topic segment | `foo/+/baz` |
| # | Matches zero or more segments at the end of the pattern | `foo/#` |

When publishing, the topic MUST use URL escaping for any wildcard characters in the topic. Subscribers MUST also escape `+` and `#` characters that are not intended as wildcards in topic subscriptions.

Topic structure exists for subscription matching only. A server MUST NOT interpret segments as carrying application semantics, and MUST NOT require clients to follow any naming schema beyond what this section defines.

### ActivityPub topics

For ActivityPub entities, topics correspond to ActivityPub HTTP/HTTPS URIs. To construct a topic from a URI, use the following algorithm.

1. Remove the URL scheme.
2. The URL authority (domain and optional port) is the first segment of the topic.
3. The URL path becomes "/"-separated path segments.
4. If a URL fragment is present this becomes the final segment.

Topic wildcard characters in topic segments MUST be URL-escaped.

**Examples**

| URL | Topic |
|----|----|
| `https://server.example/note/1` | `server.example/note/1` |
| `https://server.example:1000/actor#xyz` | `server.example:1000/actor/xyz` |
| `https://server.example:1000/actor+123#xyz` | `server.example:1000/actor%2B123/xyz` |

## Session Management

### Create Session — `POST {streamingControl}`

Requires: authenticated Principal.

The server:

1. Generates a cryptographically random ticket.
2. Records SSE session state (ticket, Principal, origin, etc.).
3. Sets a `Set-Cookie` response header carrying the ticket value with the implementation's configured `Path`, `SameSite`, `Secure` and `HttpOnly` attributes.
4. Returns `201 Created` with a JSON body.

**Server Response body:**

| Field | Type | Description |
|-------|------|-------------|
| `subscriptions_url` | string | **Required.** Absolute URL of the subscriptions endpoint. Clients MUST use this value for all subscription management requests in this session. |
| `stream_url` | string | **Required.** Absolute URL of the SSE stream endpoint. Clients MUST use this value to open the event stream. |
| `expires_at` | string | ISO 8601 UTC timestamp of ticket expiry. |
| `wildcard_support` | bool | Indicates whether the server supports topic wildcards. Defaults to `true` if not provided. |

Servers MUST include `subscriptions_url` and `stream_url` in every `201 Created` response to this endpoint. Clients MUST treat these URLs as opaque and MUST NOT attempt to derive or construct them from the SSE prefix or any other path convention.

**Example**

```json
{
  "subscriptions_url": "https://server.example/sse/control/subscriptions",
  "stream_url": "https://server.example/sse/stream",
  "expires_at": "2001-03-10T12:05:00Z",
  "wildcard_support": true
}
```

**Cookie attributes (RECOMMENDED defaults):**

| Attribute | Recommended value |
|-----------|------------------|
| `Path` | Scoped to the implementation's streaming API path |
| `SameSite` | `Lax` |
| `Secure` | `true` in production |
| `HttpOnly` | The server MAY set this to `false` to allow JavaScript access |
| `Max-Age` | Cookie expiry age |

The ticket MUST expire after a server-configured TTL. Clients MUST call `POST {streamingControl}` again to renew before opening a new stream.

### Revoke Session — `DELETE {streamingControl}`

Requires: authenticated Principal. The ticket cookie is read if present.

The server:
1. Invalidates the ticket.
2. Sends a `Set-Cookie` header that clears the cookie (`Max-Age=0`).
3. Returns `204 No Content` if successful.

Client implementations SHOULD close any active SSE connections associated with the revoked ticket.

## Subscription Management

All subscription endpoints require both:
- A valid authentication credential.
- A valid, non-expired ticket cookie associated with the authenticated Principal.

Missing either condition MUST result in `401 Unauthorized`.

### List Subscriptions — `GET {subscriptions_url}`

**Response:**

```json
{
  "topics": ["notifications", "timeline:home"]
}
```

| Field | Type  | Description |
|-------|---|-------------|
| `topics` | string&nbsp;array | The full set of subscribed topics. An empty array indicates no active subscriptions. |


### Add Subscription — `POST {subscriptions_url}`

**Request body:**

```json
{
  "topics": [
    "remote-server.example/note/abcd",
    "server.example/note/#"
  ]
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `topics` | string&nbsp;array | **Required.** | The topic identifiers or patterns |

**Response:**

```json
{
  "topics": [
    "remote-server.example/note/abcd",
    "server.example/note/#",
    "server.example/another-topic"
  ]
}
```

| Field | Type | Description |
|-------|------|-------|
| `topics` | string&nbsp;array | The full set of subscribed topics. The array reflects the full set of subscriptions after the operation. Adding a topic that is already subscribed is idempotent and MUST NOT return an error. |

### Remove Subscription — `DELETE {subscriptions_url}?topic={topic}`

The `topic` query parameter is required. Removing a non-existent topic MUST be idempotent (no error).

**Response:**

- If successful, the server MUST respond with `204 No Content`.

## SSE Stream Endpoint

### Opening the Stream — `GET {stream_url}`

Requires:
- A valid authentication credential.
- A valid, non-expired ticket cookie whose associated identity matches the Principal.

The client MUST send `Accept: text/event-stream`.

On success the server:
1. Responds with `Content-Type: text/event-stream` and keeps the response body open.
2. Sets `Cache-Control: no-cache` and (if behind a reverse proxy) `X-Accel-Buffering: no`.
3. Begins delivering `ServerSentEvent` frames for all topics the Principal is currently subscribed to.
4. Keeps the connection open until the client disconnects, the ticket expires, the SSE session is revoked, or the server restarts.

Only one active stream per Principal SHOULD be supported; if a second stream is opened the server MAY close the first.

The ticket is single-use. The server MUST only create one stream per ticket.

### ActivityPub SSE Event Format

Each event follows the [W3C EventSource] wire format:

```
id: <event-id>\n
event: activitypub\n
data: <JSON-object>\n
\n
```

| SSE field | Value |
|-----------|-------|
| `id` | The `id` field of the payload object, or a server-generated identifier (a UUID without hyphens is a reasonable choice) |
| `event` | `activitypub` (implementations MAY define additional named event types) |
| `data` | A JSON-serialized ActivityPub entity |

**Example ActivityPub Event Payload (data)**

```json
{
  "@context":"https://www.w3.org/ns/activitystreams",
  "id": "https://example.com/activities/abc123",
  "type": "Create",
  "published": "2026-03-10T11:00:00Z",
  "object": { ... }
}
```

### Heartbeats

Servers SHOULD send periodic SSE heartbeat events (`event: heartbeat`) at regular intervals (RECOMMENDED: every 15 to 30 seconds) to prevent proxy timeouts and help clients detect dead connections. The `id` and `data` fields are optional in these events.

## Security Considerations

### Ticket Security

- Tickets MUST be generated using a cryptographically secure random source with sufficient entropy to resist guessing attacks.
- Servers SHOULD limit the number of live tickets per Principal (for example, the most recent few) and SHOULD rate-limit `POST {streamingControl}` per Principal, answering `429 Too Many Requests` when the limit is exceeded. Issuing a new ticket beyond the limit SHOULD revoke the oldest.
- Tickets MUST be invalidated on SSE session revocation (`DELETE {streamingControl}`).
- Servers MUST enforce ticket TTL; expired tickets MUST be rejected.
- Ticket values MUST NOT appear in server access logs in plain form.

### Cookie Security

- The ticket cookie SHOULD be `Secure` (HTTPS only) in production.
- `SameSite=Lax` or `SameSite=Strict` MUST be set to mitigate CSRF.
- `HttpOnly` SHOULD be set unless the client is a first-party JavaScript application that must read the cookie.

### Authorization

- The server MUST verify that the ticket's associated Principal matches the currently authenticated Principal on every request that requires a ticket. Mismatches MUST result in `401 Unauthorized`.
- Subscriptions are scoped to the authenticated Principal; one Principal MUST NOT be able to read or modify another Principal's subscriptions.

### Topic Namespace

- Servers SHOULD validate topic strings against an allowlist or length/character constraints to prevent abuse.

### Transport Security

- All endpoints MUST be served over HTTPS in production.
- SSE connections SHOULD be protected against cross-origin misuse via [`CORS`][CORS] headers restricting `Origin` to trusted first-party origins.

- If the client application is served from a different first-party origin (for example `app.example` for the client and `api.example` for the API), CORS is required and MUST be strict:
  1. The server MUST send the exact client origin in `Access-Control-Allow-Origin`, never `*`.
  2. The server MUST include `Access-Control-Allow-Credentials: true` if cookies are used.
  3. The server SHOULD send `Vary: Origin`.
  4. The server SHOULD validate `Origin` against an allowlist and reject all others.


## Privacy Considerations

The SSE stream may carry sensitive information (notifications, timeline events). Implementations MUST:
- Restrict stream access to the owning Principal.
- Limit streaming events to content that the Principal is authorized to view.
- Not include events from topics the Principal has not explicitly subscribed to.
- Consider data minimization: deliver only the fields necessary for the client's use case.

## Implementations

- [FIRM](https://github.com/steve-bate/firm)

## Alternative Approaches

Another common technique for implementing event streams is to use [WebSockets](https://developer.mozilla.org/en-US/docs/Web/API/WebSockets_API). SSE has several advantages over WebSockets for streaming purposes.

- **Native unidirectional model**: SSE is designed exactly for server→client event streams (logs, notifications, token streaming, dashboards). A one-way feed needs no return path, so there is no idle reverse channel to keep open.

- **Runs over plain HTTP**: Messages are just a long-lived HTTP response using text/event-stream, so it works naturally with existing HTTP infrastructure (proxies, load balancers, CDNs) without WebSocket upgrade handling.

- **Simpler implementation and debugging**: On the server you just write to the response stream; on the client you use the EventSource API. No custom framing or libraries are needed, which reduces code size and potential bugs.

- **Better proxy/firewall compatibility**: Because it is just HTTP, SSE usually passes through corporate firewalls and middleboxes more reliably than WebSocket upgrades, which some environments still block or interfere with.

- **HTTP/2 friendliness**: With HTTP/2 a client can multiplex multiple SSE streams over a single TCP connection, leveraging the same optimizations as the rest of its HTTP traffic, whereas WebSockets use a separate protocol and do not get the same native multiplexing.

[ActivityPub]: https://www.w3.org/TR/activitypub/ "The ActivityPub protocol is a decentralized social networking protocol based upon the ActivityStreams 2.0 data format. It provides a client to server API for creating, updating and deleting content, as well as a federated server to server API for delivering notifications and content."
[SSE]: https://html.spec.whatwg.org/multipage/server-sent-events.html "This specification enables servers to push data to web pages over HTTP or using dedicated server-push protocols."
[ActivityVocabulary]: https://www.w3.org/TR/activitystreams-core/ "This specification describes the Activity vocabulary. It is intended to be used in the context of the ActivityStreams 2.0 format and provides a foundational vocabulary for activity structures, and specific activity types."
[OAUTH2]: https://oauth.net/2/ "OAuth 2.0 is the industry-standard protocol for authorization. OAuth 2.0 focuses on client developer simplicity while providing specific authorization flows for web applications, desktop applications, mobile phones, and living room devices."
[RFC-2119]: https://tools.ietf.org/html/RFC-2119.html "Key words for use in RFCs to Indicate Requirement Levels"
[W3C EventSource]: https://html.spec.whatwg.org/multipage/server-sent-events.html
[MQTT Wildcards]: https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901241
[CORS]: https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/CORS

## Copyright

CC0 1.0 Universal (CC0 1.0) Public Domain Dedication

To the extent possible under law, the authors of this Fediverse Enhancement Proposal have waived all copyright and related or neighboring rights to this work.
