# FEP-3ab2: ActivityPub Event Streaming API

**Status:** DRAFT · **Type:** implementation · **Author:** Steve Bate · **Received:** 2026-03-14

## Overview

ActivityPub defines server-to-server federation but says nothing about how a client application
receives real-time events. This FEP specifies a lightweight, cookie-authenticated
[Server-Sent Events (SSE)](https://html.spec.whatwg.org/multipage/server-sent-events.html)
streaming API that an ActivityPub server can expose to its authenticated clients.

The API has two parts:

1. **A session-control sub-API** that issues and revokes short-lived, single-use streaming
   tickets and manages per-user topic subscriptions.
2. **A stream endpoint** that delivers a single multiplexed event feed for all of the user's
   subscribed topics.

By separating authentication (the server's existing mechanism) from stream authorization (a
short-lived ticket held in an `HttpOnly` cookie), the design avoids putting credentials in the
stream URL — where proxies and logs could leak them — and avoids the browser's per-origin
connection limit by multiplexing subscriptions onto one connection. Topics are
`/`-delimited strings with optional MQTT-style wildcards, and ActivityPub entity URIs map to
topics by dropping the scheme and splitting the authority and path into segments.

## Files

- [`fep-3ab2.md`](fep-3ab2.md) — the proposal
- [`fep-3ab2.jsonld`](fep-3ab2.jsonld) — JSON-LD context

## The FEP

- Document: <https://github.com/steve-bate/fep-3ab2-event-streaming-api/blob/main/fep-3ab2.md>
- Repository: <https://github.com/steve-bate/fep-3ab2-event-streaming-api>
- Discussion: <https://github.com/steve-bate/fep-3ab2-event-streaming-api/issues>

## License

This work is released under [CC0 1.0 Universal](LICENSE.md) (public domain dedication).
See [LICENSE.md](LICENSE.md).
