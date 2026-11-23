/**
 * Minimal cookie helpers.
 *
 * FEP-3ab2 needs exactly two operations: set the ticket cookie and clear it. The
 * attributes matter more than the parsing, so the defaults here are the secure
 * ones the spec recommends (`Secure`, `HttpOnly`, `SameSite=Lax`).
 */

export type SameSitePolicy = 'Lax' | 'Strict' | 'None';

export interface CookieOptions {
  readonly name: string;
  readonly path: string;
  readonly sameSite: SameSitePolicy;
  readonly secure: boolean;
  readonly httpOnly: boolean;
  readonly domain?: string;
  /** Used for `Max-Age`; omitted means a session cookie. */
  readonly maxAgeSeconds?: number;
}

export const DEFAULT_TICKET_COOKIE: CookieOptions = {
  name: 'sse_ticket',
  path: '/',
  sameSite: 'Lax',
  // Safe by default: a ticket grants stream access for its TTL.
  secure: true,
  httpOnly: true,
};

export function serializeCookie(value: string, options: CookieOptions): string {
  const parts = [`${options.name}=${encodeURIComponent(value)}`];

  if (options.domain !== undefined) parts.push(`Domain=${options.domain}`);
  parts.push(`Path=${options.path}`);

  if (options.maxAgeSeconds !== undefined)
    parts.push(`Max-Age=${Math.trunc(options.maxAgeSeconds)}`);
  if (options.httpOnly) parts.push('HttpOnly');
  if (options.secure) parts.push('Secure');
  parts.push(`SameSite=${options.sameSite}`);

  return parts.join('; ');
}

/**
 * Serialises the `Set-Cookie` value that deletes the cookie. `Path` and `Domain`
 * must match the ones used when setting it, or browsers keep the old cookie.
 */
export function clearCookie(options: CookieOptions): string {
  return serializeCookie('', { ...options, maxAgeSeconds: 0 });
}

export function parseCookies(
  header: string | readonly string[] | undefined,
): Record<string, string> {
  const cookies: Record<string, string> = {};
  if (header === undefined) return cookies;

  const text = typeof header === 'string' ? header : header.join('; ');

  for (const part of text.split(';')) {
    const separator = part.indexOf('=');
    if (separator < 1) continue;
    const name = part.slice(0, separator).trim();
    if (name.length === 0) continue;
    const rawValue = part.slice(separator + 1).trim();
    try {
      cookies[name] = decodeURIComponent(rawValue);
    } catch {
      cookies[name] = rawValue;
    }
  }

  return cookies;
}
