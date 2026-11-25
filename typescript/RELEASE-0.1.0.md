## @fep-3ab2/streaming 0.1.0

Client and server toolkit for the FEP-3ab2 event streaming API.

- Server: framework-agnostic streaming service with ticketed sessions,
  topic subscriptions with MQTT wildcards, heartbeats and
  `application/problem+json` errors.
- Client: `StreamingClient` over `EventSource` in the browser or a
  `fetch`-based reader in Node, with reconnect and ticket renewal.
- Demo: `examples/` serves a page and an API from separate origins.

### Install from this release

```bash
npm install https://github.com/steve-bate/fep-3ab2-event-streaming-api/releases/download/typescript-v0.1.0/fep-3ab2-streaming-0.1.0.tgz
```

Or download `fep-3ab2-streaming-0.1.0.tgz` from the assets below and install the file:

```bash
npm install ./fep-3ab2-streaming-0.1.0.tgz
```

### Import it

The archive installs under its package name, so the imports do not change:

```ts
// Client: discover the control endpoint, then open one stream for any number of topics.
import { StreamingClient, discoverStreamingEndpoint } from '@fep-3ab2/streaming/client';

const streamingControl = await discoverStreamingEndpoint('https://server.example/users/alice');
const client = new StreamingClient({ streamingControl, topics: ['notifications'] });
client.on('activity', (activity) => console.log(activity.id, activity.data));
client.on('state', (state) => console.log('stream state:', state));
await client.open();
```

```ts
// Server: the control API and the SSE endpoint as plain functions.
import { createStreamingService } from '@fep-3ab2/streaming/server';

const service = createStreamingService({ publicOrigin: 'https://api.example' });
```

Entry points: `@fep-3ab2/streaming` (an alias of `/topics`), `/topics`, `/client`, `/server`, `/testing`. They are ES modules behind an `exports` map, so TypeScript needs `"module": "nodenext"` or `"moduleResolution": "bundler"` to resolve them.

The archive holds the built `dist/` and the README. There is no registry copy to upgrade from, so a newer version requires installing a newer archive. Requires Node 22 or later.
