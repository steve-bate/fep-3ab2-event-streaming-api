/**
 * Shared fixtures for the server and client suites.
 */

import {
  createStreamingService,
  type AuthenticatedRequest,
  type HttpHeaders,
  type StreamingService,
  type StreamingServiceOptions,
} from '@fep-3ab2/streaming/server';

export const PRINCIPAL = 'https://api.example/users/alice';
export const OTHER_PRINCIPAL = 'https://api.example/users/bob';
export const API_ORIGIN = 'https://api.example';

/** A service with predictable settings: no heartbeats, no Secure cookie. */
export function testService(
  options: StreamingServiceOptions & { readonly omitPublicOrigin?: boolean } = {},
): StreamingService {
  const { omitPublicOrigin, ...rest } = options;
  return createStreamingService({
    ...(omitPublicOrigin === true ? {} : { publicOrigin: API_ORIGIN }),
    heartbeatIntervalMs: 0,
    ticket: { cookie: { secure: false } },
    ...rest,
  });
}

/** The full `Set-Cookie` value from a response. */
export function setCookie(headers: HttpHeaders): string {
  const raw = headers['set-cookie'];
  const value = Array.isArray(raw) ? raw[0] : raw;
  if (value === undefined) throw new Error('response carried no set-cookie header');
  return value;
}

/** The `name=value` pair, ready to be sent back as a `Cookie` header. */
export function ticketCookie(headers: HttpHeaders): string {
  return setCookie(headers).split(';')[0] ?? '';
}

export function buildRequest(
  cookie?: string,
  extra: Partial<AuthenticatedRequest> = {},
): AuthenticatedRequest {
  return {
    principal: PRINCIPAL,
    // Every real client asks for an event stream; the service answers 406 without
    // `Accept: text/event-stream` (FEP-3ab2 §Opening the Stream).
    headers: { accept: 'text/event-stream', ...(cookie === undefined ? {} : { cookie }) },
    ...extra,
  };
}

export async function withTimeout<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      reject(new Error(`Timed out after ${ms} ms waiting for ${label}`));
    }, ms);
  });

  try {
    return await Promise.race([promise, timeout]);
  } finally {
    if (timer !== undefined) clearTimeout(timer);
  }
}

/** Reads the next SSE chunk, failing if the stream ended first. */
export async function nextFrame(
  iterator: AsyncIterator<string>,
  label = 'an SSE frame',
): Promise<string> {
  const result = await withTimeout(iterator.next(), 1_000, label);
  if (result.done === true) throw new Error(`The stream ended while waiting for ${label}`);
  return result.value;
}

/** Collects frames until `count` arrive. */
export async function nextFrames(
  iterator: AsyncIterator<string>,
  count: number,
  label = 'SSE frames',
): Promise<string[]> {
  const frames: string[] = [];
  for (let index = 0; index < count; index += 1) {
    frames.push(await nextFrame(iterator, label));
  }
  return frames;
}

export function jsonBody(frame: string): unknown {
  const data = frame
    .split('\n')
    .filter((line) => line.startsWith('data: '))
    .map((line) => line.slice('data: '.length))
    .join('\n');
  return JSON.parse(data) as unknown;
}
