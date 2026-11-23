/**
 * The example's FEP-3ab2 service: an Express 5 app that shows every part of the
 * protocol: an actor document advertising `endpoints.streamingControl`, the
 * session/subscription control API, the SSE stream, and strict CORS for the
 * separately served demo page.
 *
 * Authentication is minimal (a session cookie set from HTTP Basic) because the
 * FEP does not prescribe one. The streaming service never sees the auth
 * mechanism: it only receives a principal URI.
 */

import { once } from 'node:events';
import express, { type NextFunction, type Request, type Response } from 'express';
import {
  PROBLEM_MEDIA_TYPE,
  PROBLEM_TYPES,
  createStreamingService,
  problem,
  type ProblemDetails,
  type SameSitePolicy,
  type ServiceResult,
  type StreamingService,
} from '@fep-3ab2/streaming/server';
import { topicFromUri } from '@fep-3ab2/streaming/topics';

export interface ApiServerOptions {
  /**
   * Origins the demo page may be served from. Both loopback spellings are listed
   * by the caller, because `localhost` and `127.0.0.1` are different origins to a
   * browser (and different cookie hosts).
   */
  readonly webOrigins: readonly string[];
  /**
   * Origin the API advertises in the session response. Omit it to derive the
   * origin from each request's Host header, so the demo works
   * under either loopback spelling.
   */
  readonly publicOrigin?: string;
  readonly ticketTtlSeconds?: number;
  readonly heartbeatIntervalMs?: number;
  /** `Secure` on the ticket cookie; needs https in the browser. */
  readonly cookieSecure?: boolean;
  /**
   * Ticket cookie `SameSite`. `Lax` (the default) carries across same-site
   * subdomains; a page on another registrable domain needs `None`.
   */
  readonly cookieSameSite?: SameSitePolicy;
  /** Demo credentials, `username → password`. Defaults to alice / demo. */
  readonly users?: Readonly<Record<string, string>>;
  readonly log?: boolean;
}

export interface ApiServer {
  readonly app: express.Express;
  readonly service: StreamingService;
  stop(): void;
}

/** Demo credentials. A real deployment plugs in its own session or OAuth. */
const DEFAULT_USERS: Readonly<Record<string, string>> = { alice: 'demo' };
const SESSION_COOKIE = 'demo_session';

const PATHS = {
  control: '/sse/control',
  subscriptions: '/sse/control/subscriptions',
  stream: '/sse/stream',
} as const;

export function createApiServer(options: ApiServerOptions): ApiServer {
  const users = options.users ?? DEFAULT_USERS;
  const app = express();
  app.disable('x-powered-by');
  // Behind a reverse proxy the TLS terminates there, so `req.protocol` and the
  // actor document's own URLs are only https when the forwarded headers are
  // trusted. Only do this where the app is not directly reachable.
  app.set('trust proxy', true);
  app.use(
    express.json({
      limit: '64kb',
      type: ['application/json', 'application/activity+json', 'application/ld+json'],
    }),
  );

  const service = createStreamingService({
    // Without a configured `publicOrigin` the service derives it from the request's
    // Host header, so the advertised stream URL matches whichever name the browser
    // used to reach us.
    ...(options.publicOrigin === undefined ? {} : { publicOrigin: options.publicOrigin }),
    urls: PATHS,
    // A short TTL, so ticket renewal is visible during a demo session.
    ticket: {
      ttlSeconds: options.ticketTtlSeconds ?? 120,
      cookie: {
        path: '/sse',
        secure: options.cookieSecure ?? false,
        sameSite: options.cookieSameSite ?? 'Lax',
        httpOnly: true,
      },
    },
    heartbeatIntervalMs: options.heartbeatIntervalMs ?? 10_000,
    cors: { allowedOrigins: options.webOrigins },
    ...(options.log === true
      ? {
          logger: {
            debug: () => undefined,
            info: (message: string, details?: Record<string, unknown>) => {
              console.log(`[stream] ${message}`, details ?? '');
            },
            warn: (message: string, details?: Record<string, unknown>) => {
              console.warn(`[stream] ${message}`, details ?? '');
            },
          },
        }
      : {}),
  });

  /**
   * The demo session cookie. It mirrors the ticket cookie's `SameSite`/`Secure`
   * policy: a cross-site page needs `None` on both, or the control API answers
   * 401 while the page believes it is signed in.
   */
  function sessionCookie(value: string, maxAgeSeconds?: number) {
    const parts = [
      `${SESSION_COOKIE}=${encodeURIComponent(value)}`,
      'Path=/',
      'HttpOnly',
      `SameSite=${options.cookieSameSite ?? 'Lax'}`,
    ];
    if (options.cookieSecure === true) parts.push('Secure');
    if (maxAgeSeconds !== undefined) parts.push(`Max-Age=${maxAgeSeconds}`);
    return parts.join('; ');
  }

  /** The origin the client actually used, like `http://127.0.0.1:5181`. A configured
   * `publicOrigin` wins: behind a proxy that is the one name the browser sees.
   */
  function originOf(req: Request): string {
    if (options.publicOrigin !== undefined) return options.publicOrigin;
    return `${req.protocol}://${req.get('host') ?? 'localhost'}`;
  }

  const actorFor = (username: string, origin: string) => `${origin}/users/${username}`;

  /** Resolves the demo session cookie to a principal URI. */
  function principalOf(req: Request) {
    const username = parseCookieHeader(req.headers.cookie)[SESSION_COOKIE];
    if (username === undefined || users[username] === undefined) return undefined;
    return actorFor(username, originOf(req));
  }

  function requestOf(req: Request, signal?: AbortSignal) {
    const principal = principalOf(req);
    if (principal === undefined) {
      throw new Error('requestOf() needs an authenticated request');
    }
    return {
      principal,
      headers: req.headers,
      ...(req.headers.origin === undefined ? {} : { origin: req.headers.origin }),
      ...(signal === undefined ? {} : { signal }),
    };
  }

  // CORS: exact origin, credentials, Vary
  app.use((req, res, next) => {
    const decision = service.cors(req.headers.origin);
    if (!decision.allowed) {
      sendProblem(res, 403, 'The request origin is not allowed', PROBLEM_TYPES.originNotAllowed);
      return;
    }
    for (const [name, value] of Object.entries(decision.headers)) res.setHeader(name, value);
    next();
  });

  // Preflight for the JSON POSTs and the DELETE with a query string.
  app.options(/.*/, (req, res) => {
    const result = service.preflight({
      ...(req.headers.origin === undefined ? {} : { origin: req.headers.origin }),
      ...(req.headers['access-control-request-method'] === undefined
        ? {}
        : { requestMethod: req.headers['access-control-request-method'] }),
      ...(req.headers['access-control-request-headers'] === undefined
        ? {}
        : { requestHeaders: req.headers['access-control-request-headers'] }),
    });
    res.status(result.status);
    for (const [name, value] of Object.entries(result.headers)) res.setHeader(name, value);
    res.end();
  });

  // Demo auth
  app.post('/demo/login', (req, res) => {
    const credentials = parseBasicAuth(req.headers.authorization);
    if (credentials === undefined || users[credentials.username] !== credentials.password) {
      sendProblem(res, 401, 'Invalid credentials');
      return;
    }

    res.setHeader('set-cookie', sessionCookie(credentials.username));
    res.status(200).json({ actor: actorFor(credentials.username, originOf(req)) });
  });

  app.post('/demo/logout', (_req, res) => {
    res.setHeader('set-cookie', sessionCookie('', 0));
    res.status(204).end();
  });

  // Actor document (endpoint discovery)
  app.get('/users/:username', (req, res) => {
    const username = req.params.username;
    if (users[username] === undefined) {
      sendProblem(res, 404, `No such actor: ${username}`);
      return;
    }

    const origin = originOf(req);
    const actor = actorFor(username, origin);

    res.type('application/activity+json').json({
      '@context': ['https://w3id.org/fep/3ab2', 'https://www.w3.org/ns/activitystreams'],
      id: actor,
      type: 'Person',
      preferredUsername: username,
      inbox: `${actor}/inbox`,
      outbox: `${actor}/outbox`,
      endpoints: {
        streamingControl: `${origin}${PATHS.control}`,
      },
    });
  });

  // Streaming control API
  const requirePrincipal = (req: Request, res: Response, next: NextFunction) => {
    if (principalOf(req) === undefined) {
      sendProblem(res, 401, 'Sign in first');
      return;
    }
    next();
  };

  app.post(PATHS.control, requirePrincipal, async (req, res) => {
    send(res, await service.createSession(requestOf(req)));
  });

  app.delete(PATHS.control, requirePrincipal, async (req, res) => {
    send(res, await service.revokeSession(requestOf(req)));
  });

  app.get(PATHS.subscriptions, requirePrincipal, async (req, res) => {
    send(res, await service.listSubscriptions(requestOf(req)));
  });

  app.post(PATHS.subscriptions, requirePrincipal, async (req, res) => {
    send(res, await service.addSubscriptions({ ...requestOf(req), body: req.body }));
  });

  app.delete(PATHS.subscriptions, requirePrincipal, async (req, res) => {
    const topic = typeof req.query['topic'] === 'string' ? req.query['topic'] : undefined;
    send(res, await service.removeSubscription({ ...requestOf(req), topic }));
  });

  // SSE stream
  app.get(PATHS.stream, requirePrincipal, async (req, res) => {
    const controller = new AbortController();
    req.on('close', () => {
      controller.abort();
    });

    const result = await service.openStream(requestOf(req, controller.signal));
    if (!result.ok) {
      res.status(result.status);
      for (const [name, value] of Object.entries(result.headers)) res.setHeader(name, value);
      res.json(result.body);
      return;
    }

    res.writeHead(result.status, result.headers);
    res.flushHeaders();
    req.socket.setNoDelay(true);
    req.socket.setTimeout(0);

    try {
      for await (const chunk of result.frames) {
        if (!res.write(chunk)) await once(res, 'drain');
      }
    } catch (error) {
      if (!controller.signal.aborted) console.error('[stream] frame error', error);
    } finally {
      res.end();
    }
  });

  // A method the protocol does not define on a streaming path is a 405 with an
  // about:blank problem. Registered after the real routes so it only catches
  // what they did not.
  app.all([PATHS.control, PATHS.subscriptions, PATHS.stream], (req, res) => {
    sendProblem(res, 405, `${req.method} is not allowed on ${req.path}`);
  });

  // Demo-only endpoints
  app.get('/demo/me', (req, res) => {
    const principal = principalOf(req);
    if (principal === undefined) {
      sendProblem(res, 401, 'Sign in first');
      return;
    }
    res.json({ actor: principal });
  });

  /**
   * The actor's inbox. A real server would verify an HTTP signature here; the demo
   * accepts a session-authenticated POST and does what inbox processing does:
   * publish the received activity to the inbox topic, so subscribers see it on
   * their stream. Inbox in, stream out: the path the FEP exists to support.
   */
  app.post('/users/:username/inbox', requirePrincipal, async (req, res) => {
    const body = req.body as unknown;
    const activity = asActivity(body);
    if (activity === undefined) {
      sendProblem(res, 400, 'Expected an activity with string id and type');
      return;
    }

    const username = String(req.params.username ?? '');
    const inbox = `${originOf(req)}/users/${username}/inbox`;
    const topic = topicFromUri(inbox);
    const delivered = await service.publish(topic, activity);

    res.status(202).json({ topic, delivered });
  });

  return {
    app,
    service,
    stop: () => {
      service.closeAll();
    },
  };
}

/** Accepts only an object carrying the two fields every activity needs. */
function asActivity(value: unknown) {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return undefined;
  const candidate = value as Record<string, unknown>;
  const id = candidate['id'];
  const type = candidate['type'];
  if (typeof id !== 'string' || id.length === 0) return undefined;
  if (typeof type !== 'string' || type.length === 0) return undefined;
  return candidate;
}

function sendProblem(
  res: Response,
  status: number,
  detail: string,
  type: string = PROBLEM_TYPES.aboutBlank,
) {
  const body: ProblemDetails = problem(type, status, detail);
  res.status(status).type(PROBLEM_MEDIA_TYPE).json(body);
}

function send(res: Response, result: ServiceResult<unknown>) {
  res.status(result.status);
  for (const [name, value] of Object.entries(result.headers)) res.setHeader(name, value);
  if (result.status === 204 || result.body === null) {
    res.end();
    return;
  }
  res.json(result.body);
}

function parseBasicAuth(
  header: string | undefined,
): { username: string; password: string } | undefined {
  const value = header ?? '';
  if (!value.toLowerCase().startsWith('basic ')) return undefined;
  const decoded = Buffer.from(value.slice(6), 'base64').toString('utf8');
  const separator = decoded.indexOf(':');
  if (separator < 1) return undefined;
  return { username: decoded.slice(0, separator), password: decoded.slice(separator + 1) };
}

function parseCookieHeader(header: string | undefined) {
  const out: Record<string, string> = {};
  if (header === undefined) return out;
  for (const part of header.split(';')) {
    const separator = part.indexOf('=');
    if (separator < 1) continue;
    const name = part.slice(0, separator).trim();
    try {
      out[name] = decodeURIComponent(part.slice(separator + 1).trim());
    } catch {
      out[name] = part.slice(separator + 1).trim();
    }
  }
  return out;
}
