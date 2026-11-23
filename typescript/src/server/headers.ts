/**
 * Header helpers shared by the service and the adapters.
 *
 * Node's `IncomingHttpHeaders` may give a header as `string | string[]`, and
 * Express passes that shape straight through, so every header read goes through
 * these functions instead of indexing the record.
 */

import type { HeadersLike } from './types.js';

/** Reads a header case-insensitively, collapsing repeated values to the first. */
export function readHeader(headers: HeadersLike, name: string): string | undefined {
  const lower = name.toLowerCase();
  const value = headers[lower] ?? headers[name];
  if (value === undefined) return undefined;
  if (typeof value === 'string') return value;

  const first = value[0];
  return typeof first === 'string' ? first : undefined;
}

/** Reads the first element of a possibly comma-separated header (`a, b` → `a`). */
export function firstHeaderValue(value: string | undefined): string | undefined {
  if (value === undefined) return undefined;
  const first = value.split(',')[0];
  return first === undefined ? undefined : first.trim();
}
