/**
 * Demo configuration, from `typescript/.env` and the process environment.
 *
 * Nothing here is required: every value has a default that runs the demo on
 * loopback. Set the origins when the demo is served from a real host name or
 * behind a proxy, so the API advertises URLs and allowlists origins that
 * match the name the browser actually used.
 *
 * The `.env` file is not committed; `.env.example` lists the supported keys.
 */

import { existsSync } from 'node:fs';
import path from 'node:path';
import type { SameSitePolicy } from '@fep-3ab2/streaming/server';

/** `typescript/.env`, next to the package that owns the demo */
const DEFAULT_ENV_FILE = path.join(import.meta.dirname, '..', '.env');

export interface DemoConfig {
  /** Absolute origin the API advertises, e.g. `https://api.example`. */
  readonly apiOrigin: string | undefined;
  readonly apiPort: number;
  /** Absolute origin the demo page is served from */
  readonly webOrigin: string | undefined;
  readonly webPort: number;
  /** Exact origins allowed to call the API (strict CORS) */
  readonly webOrigins: readonly string[];
  /** Mount the API on the page's origin instead of a second port */
  readonly singleOrigin: boolean;
  readonly log: boolean;
  readonly ticketTtlSeconds: number;
  readonly heartbeatIntervalMs: number;
  /** `Secure` on the ticket cookie: on whenever the API origin is https */
  readonly cookieSecure: boolean;
  /**
   * Ticket cookie `SameSite`. `Lax` carries across a same-site page (two
   * subdomains of one registrable domain); a page on a *different* domain needs
   * `None`, which browsers only honour together with `Secure`.
   */
  readonly cookieSameSite: SameSitePolicy;
  /** `username → password`. */
  readonly users: Readonly<Record<string, string>>;
  /** No allowlist: trust comes only from the client behind the token. */
  readonly clientOnly: boolean;
  /** Clients that may ask for a token, each with the origin it registered. */
  readonly clients: readonly { readonly clientId: string; readonly origin: string }[];
}

/**
 * Loads `.env` (unless `DEMO_ENV_FILE` names another file) and parses the result.
 * Real environment variables win over the file's values.
 */
export function loadConfig(): DemoConfig {
  const envFile = process.env['DEMO_ENV_FILE'] ?? DEFAULT_ENV_FILE;
  if (existsSync(envFile)) process.loadEnvFile(envFile);
  return parseConfig(process.env);
}

/** Pure form of {@link loadConfig}, for tests and for callers that pass their own env. */
export function parseConfig(env: NodeJS.ProcessEnv): DemoConfig {
  const apiPort = readPort(env, 'DEMO_API_PORT', 5181);
  const webPort = readPort(env, 'DEMO_WEB_PORT', 5180);
  const apiOrigin = readOrigin(env, 'DEMO_API_ORIGIN');
  const webOrigin = readOrigin(env, 'DEMO_WEB_ORIGIN');

  const listed = (env['DEMO_WEB_ORIGINS'] ?? '')
    .split(',')
    .map((value) => value.trim())
    .filter((value) => value.length > 0)
    .map((value) => toOrigin(value, 'DEMO_WEB_ORIGINS'));

  const webOrigins: readonly string[] =
    listed.length > 0
      ? listed
      : webOrigin !== undefined
        ? [webOrigin]
        : [`http://localhost:${webPort}`, `http://127.0.0.1:${webPort}`];

  const cookieSecure = apiOrigin?.startsWith('https://') ?? false;
  const cookieSameSite = readSameSite(env, 'DEMO_COOKIE_SAMESITE', 'Lax');
  if (cookieSameSite === 'None' && !cookieSecure) {
    throw new Error(
      'DEMO_COOKIE_SAMESITE=None needs an https DEMO_API_ORIGIN: browsers reject a ' +
        'SameSite=None cookie that is not Secure, so the ticket would never be stored',
    );
  }

  // `id@origin` pairs, so a demo can try a client the allowlist never heard of.
  const clients = (env['DEMO_CLIENTS'] ?? '')
    .split(',')
    .map((entry) => entry.trim())
    .filter((entry) => entry.length > 0)
    .map((entry) => {
      const at = entry.indexOf('@');
      if (at <= 0) throw new Error(`DEMO_CLIENTS entries look like id@origin: ${entry}`);
      return {
        clientId: entry.slice(0, at),
        origin: toOrigin(entry.slice(at + 1), 'DEMO_CLIENTS'),
      };
    });

  const user = env['DEMO_USER']?.trim() ?? '';
  const password = env['DEMO_PASSWORD'] ?? '';

  return {
    apiOrigin,
    apiPort,
    webOrigin,
    webPort,
    webOrigins,
    singleOrigin: env['DEMO_SINGLE_ORIGIN'] === '1',
    log: env['DEMO_LOG'] !== '0',
    ticketTtlSeconds: readInteger(env, 'DEMO_TICKET_TTL_SECONDS', 120, 1),
    heartbeatIntervalMs: readInteger(env, 'DEMO_HEARTBEAT_INTERVAL_MS', 10_000, 0),
    cookieSecure,
    cookieSameSite,
    users: { [user.length > 0 ? user : 'alice']: password.length > 0 ? password : 'demo' },
    clients,
    clientOnly: env['DEMO_CLIENT_ONLY'] === '1',
  };
}

function readPort(env: NodeJS.ProcessEnv, name: string, fallback: number) {
  const value = readInteger(env, name, fallback, 1);
  if (value > 65_535) throw new Error(`${name} must be a port number (1-65535), got ${value}`);
  return value;
}

function readInteger(env: NodeJS.ProcessEnv, name: string, fallback: number, minimum: number) {
  const raw = env[name]?.trim();
  if (raw === undefined || raw.length === 0) return fallback;

  const value = Number(raw);
  if (!Number.isInteger(value) || value < minimum) {
    throw new Error(`${name} must be an integer >= ${minimum}, got "${raw}"`);
  }
  return value;
}

function readSameSite(env: NodeJS.ProcessEnv, name: string, fallback: SameSitePolicy) {
  const raw = env[name]?.trim();
  if (raw === undefined || raw.length === 0) return fallback;

  switch (raw.toLowerCase()) {
    case 'lax':
      return 'Lax';
    case 'strict':
      return 'Strict';
    case 'none':
      return 'None';
    default:
      throw new Error(`${name} must be Lax, Strict or None, got "${raw}"`);
  }
}

function readOrigin(env: NodeJS.ProcessEnv, name: string) {
  const raw = env[name]?.trim();
  if (raw === undefined || raw.length === 0) return undefined;
  return toOrigin(raw, name);
}

function toOrigin(value: string, name: string) {
  let url: URL;
  try {
    url = new URL(value);
  } catch {
    throw new Error(`${name} must be an absolute URL, got "${value}"`);
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`${name} must be http or https, got "${value}"`);
  }
  return url.origin;
}
