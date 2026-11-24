/**
 * Strict CORS for cross-origin first-party deployments.
 *
 * The rules the spec asks for, in order: the exact SPA origin, never `*`, then
 * `Access-Control-Allow-Credentials: true`, a `Vary: Origin` so caches do not
 * mix answers, and an allowlist that rejects everything else. An omitted policy
 * means "same origin only", where the browser enforces the boundary itself.
 */

import type { HttpHeaders } from './types.js';

export interface CorsPolicy {
  /** Exact origins, like `['https://app.example']`. No wildcards, no trailing slash. */
  readonly allowedOrigins: readonly string[];
  /** Credentialed requests are the norm for cookie-based sessions. Default true. */
  readonly allowCredentials?: boolean;
  /** Preflight cache lifetime in seconds. Default 600. */
  readonly maxAgeSeconds?: number;
}

export interface CorsDecision {
  readonly allowed: boolean;
  /** The echoed origin, when one was supplied */
  readonly origin: string | undefined;
  readonly headers: HttpHeaders;
}

export interface PreflightRequest {
  readonly origin?: string;
  readonly requestMethod?: string;
  readonly requestHeaders?: string;
  /**
   * Set by an adapter that has already approved an origin its policy does not
   * name, so the preflight answers the same way the real request will.
   */
  readonly trusted?: boolean;
}

/**
 * Evaluates a request's `Origin` against the policy.
 *
 * `allowed` is false only when an `Origin` is present and not allowlisted; a
 * request with no `Origin` (same-origin fetch, server-to-server, curl) is allowed
 * and simply carries no CORS headers.
 */
export function evaluateCors(
  origin: string | undefined,
  policy: CorsPolicy | undefined,
): CorsDecision {
  if (policy === undefined || origin === undefined || origin === '') {
    return { allowed: true, origin, headers: {} };
  }

  if (!isAllowedOrigin(origin, policy.allowedOrigins)) {
    return { allowed: false, origin, headers: {} };
  }

  return { allowed: true, origin, headers: trustedOriginHeaders(origin, policy) };
}

/**
 * The headers a trusted origin gets. `evaluateCors` uses it for allowlisted
 * origins; a service with an `originTrust` hook uses it for the origins that hook
 * approves, so both paths answer the same way.
 */
export function trustedOriginHeaders(origin: string, policy: CorsPolicy | undefined): HttpHeaders {
  const headers: HttpHeaders = {
    'access-control-allow-origin': origin,
    vary: 'Origin',
  };
  if (policy?.allowCredentials ?? true) {
    headers['access-control-allow-credentials'] = 'true';
  }
  return headers;
}

export function preflightHeaders(
  decision: CorsDecision,
  policy: CorsPolicy | undefined,
  request: PreflightRequest = {},
): HttpHeaders {
  if (!decision.allowed) return {};

  return {
    ...decision.headers,
    'access-control-allow-methods': 'GET, POST, DELETE, OPTIONS',
    'access-control-allow-headers': request.requestHeaders ?? 'authorization, content-type',
    'access-control-max-age': String(policy?.maxAgeSeconds ?? 600),
  };
}

function isAllowedOrigin(origin: string, allowedOrigins: readonly string[]) {
  const candidate = normalizeOrigin(origin);
  return allowedOrigins.some((allowed) => normalizeOrigin(allowed) === candidate);
}

function normalizeOrigin(origin: string) {
  return origin.trim().replace(/\/+$/, '').toLowerCase();
}
