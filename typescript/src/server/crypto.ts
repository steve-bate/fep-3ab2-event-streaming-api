/**
 * Runtime-neutral primitives: secure randomness and hashing via WebCrypto.
 *
 * Used for ticket values (256 bits of entropy), session/event ids, and hashed
 * ticket storage keys. No `node:crypto`, so the same code runs in Node, a
 * browser and an edge worker.
 */

const HEX = '0123456789abcdef';

/** Cryptographically random lowercase hex string with `bytes * 2` characters. */
export function randomHex(bytes = 32): string {
  if (!Number.isInteger(bytes) || bytes <= 0) {
    throw new RangeError(`randomHex() expects a positive byte count, got ${bytes}`);
  }
  const buffer = new Uint8Array(bytes);
  globalThis.crypto.getRandomValues(buffer);

  let out = '';
  for (const byte of buffer) {
    out += HEX.charAt(byte >> 4);
    out += HEX.charAt(byte & 0x0f);
  }
  return out;
}

export async function sha256Hex(value: string): Promise<string> {
  const digest = await globalThis.crypto.subtle.digest('SHA-256', new TextEncoder().encode(value));
  const bytes = new Uint8Array(digest);

  let out = '';
  for (const byte of bytes) {
    out += HEX.charAt(byte >> 4);
    out += HEX.charAt(byte & 0x0f);
  }
  return out;
}

/**
 * A ticket value: 32 random bytes, enough entropy to resist guessing; 256 bits
 * is comfortably beyond any attack.
 */
export function generateTicket(): string {
  return randomHex(32);
}

/** A server-generated event id for payloads that carry none */
export function generateEventId(): string {
  return randomHex(16);
}
