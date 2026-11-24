/**
 * Runs the demo: the FEP-3ab2 API on one origin and the page on another, so the
 * cross-origin (CORS + credentialed cookie) path gets exercised.
 *
 * Configuration comes from `typescript/.env` (not committed, copy
 * `.env.example`) and from the process environment. `DEMO_SINGLE_ORIGIN=1`
 * mounts the API on the page's origin instead, the mode to use when only one
 * port is reachable, say, when you port-forward the demo from another machine.
 */

import { createApiServer } from './api/server.js';
import { loadConfig } from './config.js';
import { createWebServer } from './web/server.js';

const config = loadConfig();
const { apiPort, webPort, singleOrigin } = config;

// `localhost` and `127.0.0.1` are different origins (and different cookie hosts),
// so the demo accepts the page on either, and derives everything else from the
// request it is answering, unless an origin is configured.
const apiUrl = config.apiOrigin ?? `http://localhost:${singleOrigin ? webPort : apiPort}`;
const pageUrl = config.webOrigin ?? `http://localhost:${webPort}`;
const [demoUser, demoPassword] = Object.entries(config.users)[0] ?? ['alice', 'demo'];

const api = createApiServer({
  webOrigins: config.webOrigins,
  ...(config.apiOrigin === undefined ? {} : { publicOrigin: config.apiOrigin }),
  ticketTtlSeconds: config.ticketTtlSeconds,
  heartbeatIntervalMs: config.heartbeatIntervalMs,
  cookieSecure: config.cookieSecure,
  cookieSameSite: config.cookieSameSite,
  users: config.users,
  ...(config.clients.length === 0 ? {} : { clients: config.clients }),
  ...(config.clientOnly ? { clientOnly: true } : {}),
  log: config.log,
});

const web = createWebServer({
  apiPort,
  ...(config.apiOrigin === undefined ? {} : { apiOrigin: config.apiOrigin }),
  ...(singleOrigin ? { apiApp: api.app } : {}),
});

const apiListener = singleOrigin
  ? undefined
  : api.app.listen(apiPort, () => {
      console.log(`FEP-3ab2 API        ${apiUrl}`);
      console.log(`  actor            ${apiUrl}/users/${demoUser}`);
      console.log(`  control          ${apiUrl}/sse/control`);
      console.log(`  stream           ${apiUrl}/sse/stream`);
    });

const webListener = web.app.listen(webPort, () => {
  console.log(`demo page           ${pageUrl}`);
  console.log(
    config.clientOnly
      ? '  allowlist        none (client-bound origins only)'
      : `  allowed origins  ${config.webOrigins.join(', ')}`,
  );
  console.log(
    singleOrigin
      ? '  mode             single origin (the API is mounted on this port)'
      : `  mode             cross-origin (the API is on port ${apiPort})`,
  );
  console.log(`  ticket TTL       ${config.ticketTtlSeconds}s`);
  console.log(`  heartbeat        ${config.heartbeatIntervalMs}ms`);
  console.log(
    `  ticket cookie    SameSite=${config.cookieSameSite}${config.cookieSecure ? '; Secure' : ''}`,
  );
  console.log(`\nOpen ${pageUrl} and sign in with ${demoUser} / ${demoPassword}.\n`);
});

// Fail loudly instead of serving a half-dead demo: a port already taken usually
// means another instance is still running and would keep answering requests.
const listeners: readonly (readonly [NodeJS.EventEmitter, number])[] = [
  ...(apiListener === undefined ? [] : [[apiListener, apiPort] as const]),
  [webListener, webPort] as const,
];

for (const [listener, port] of listeners) {
  listener.on('error', (error: NodeJS.ErrnoException) => {
    if (error.code === 'EADDRINUSE') {
      console.error(`Port ${port} is already in use — stop the other demo instance and retry.`);
    } else {
      console.error(`Cannot listen on port ${port}:`, error);
    }
    api.stop();
    process.exit(1);
  });
}

function shutdown() {
  console.log('\nshutting down');
  api.stop();
  apiListener?.close();
  webListener.close();
  setTimeout(() => {
    process.exit(0);
  }, 100);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
