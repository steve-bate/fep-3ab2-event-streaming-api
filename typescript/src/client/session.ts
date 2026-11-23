/**
 * Control-API calls: session management and topic subscriptions.
 *
 * These are plain functions over `fetch` so they can be reused without the
 * `StreamingClient` state machine: a server-side bot that only manages
 * subscriptions does not need an EventSource at all.
 */

import {
  ProtocolError,
  asRecord,
  isAbsoluteUrl,
  parseCreateSessionBody,
  parseProblemDetails,
  parseTopicsBody,
} from '../protocol.js';
import { StreamingClientError, toClientError } from './errors.js';
import type { FetchLike, StreamingSession } from './types.js';

export interface SessionClientContext {
  /** The actor's `endpoints.streamingControl` URL. */
  readonly controlUrl: string;
  readonly fetch: FetchLike;
  /** Default `include`: the ticket cookie has to travel with every call. */
  readonly credentials: RequestCredentials;
  /** Extra headers, e.g. `authorization` for token-authenticated servers. */
  readonly headers?: Readonly<Record<string, string>>;
}

interface RequestOptions {
  readonly method: 'GET' | 'POST' | 'DELETE';
  readonly json?: unknown;
  readonly accept?: string;
}

export async function createStreamingSession(
  context: SessionClientContext,
): Promise<StreamingSession> {
  const response = await send(context, context.controlUrl, { method: 'POST' });
  const { body } = await parse(response);

  let parsed;
  try {
    parsed = parseCreateSessionBody(body);
  } catch (error) {
    throw toProtocolError(error, 'The session response did not match FEP-3ab2');
  }

  return {
    subscriptionsUrl: parsed.subscriptions_url,
    streamUrl: parsed.stream_url,
    expiresAt: new Date(parsed.expires_at),
    heartbeatIntervalSeconds: parsed.heartbeat_interval,
    wildcardSupport: parsed.wildcard_support,
  };
}

export async function revokeStreamingSession(context: SessionClientContext): Promise<void> {
  await send(context, context.controlUrl, { method: 'DELETE' });
}

export async function listSubscriptions(
  context: SessionClientContext,
  subscriptionsUrl: string,
): Promise<string[]> {
  const response = await send(context, subscriptionsUrl, { method: 'GET' });
  const { body } = await parse(response);
  try {
    return parseTopicsBody(body).topics;
  } catch (error) {
    throw toProtocolError(error, 'The subscriptions response did not match FEP-3ab2');
  }
}

export async function addSubscriptions(
  context: SessionClientContext,
  subscriptionsUrl: string,
  topics: readonly string[],
): Promise<string[]> {
  const response = await send(context, subscriptionsUrl, { method: 'POST', json: { topics } });
  const { body } = await parse(response);
  try {
    return parseTopicsBody(body).topics;
  } catch (error) {
    throw toProtocolError(error, 'The subscriptions response did not match FEP-3ab2');
  }
}

export async function removeSubscription(
  context: SessionClientContext,
  subscriptionsUrl: string,
  topic: string,
): Promise<void> {
  const separator = subscriptionsUrl.includes('?') ? '&' : '?';
  // encodeURIComponent escapes '%', so a segment like `actor%2B123` round-trips
  // through the server's query decoding unchanged.
  const target = `${subscriptionsUrl}${separator}topic=${encodeURIComponent(topic)}`;
  await send(context, target, { method: 'DELETE' });
}

export async function discoverStreamingControl(
  actorUri: string,
  context: Omit<SessionClientContext, 'controlUrl'>,
): Promise<string> {
  const response = await send({ ...context, controlUrl: actorUri }, actorUri, {
    method: 'GET',
    accept:
      'application/activity+json, application/ld+json; profile="https://www.w3.org/ns/activitystreams"',
  });
  const { body } = await parse(response);

  const endpoints = asRecord(asRecord(body)?.['endpoints']);
  const control = endpoints?.['streamingControl'];
  const controlUrl =
    typeof control === 'string'
      ? control
      : (stringOf(asRecord(control)?.['@id']) ?? stringOf(asRecord(control)?.['id']));

  if (controlUrl === undefined || !isAbsoluteUrl(controlUrl)) {
    throw new StreamingClientError('The actor document has no endpoints.streamingControl URL', {
      code: 'protocol',
      body,
    });
  }
  return controlUrl;
}

async function send(context: SessionClientContext, url: string, options: RequestOptions) {
  const headers: Record<string, string> = {
    accept: options.accept ?? 'application/json',
    ...context.headers,
  };

  let body: string | undefined;
  if (options.json !== undefined) {
    headers['content-type'] = 'application/json';
    body = JSON.stringify(options.json);
  }

  let response: Response;
  try {
    const init: RequestInit = {
      method: options.method,
      credentials: context.credentials,
      headers,
    };
    if (body !== undefined) init.body = body;
    response = await context.fetch(url, init);
  } catch (error) {
    throw new StreamingClientError(`Request to ${url} failed`, { code: 'transport', cause: error });
  }

  if (!response.ok) {
    const payload = await readBody(response);
    const problemDetails = parseProblemDetails(payload);
    throw new StreamingClientError(
      problemDetails === undefined
        ? `Request to ${url} failed with HTTP ${response.status}`
        : `${problemDetails.title}: ${problemDetails.detail}`,
      {
        code: 'http',
        status: response.status,
        body: payload,
        ...(problemDetails === undefined ? {} : { problem: problemDetails }),
      },
    );
  }

  return response;
}

async function parse(response: Response) {
  return { body: await readBody(response) };
}

async function readBody(response: Response) {
  const text = await response.text();
  if (text.trim().length === 0) return undefined;
  try {
    return JSON.parse(text) as unknown;
  } catch {
    return text;
  }
}

function toProtocolError(error: unknown, message: string) {
  if (error instanceof ProtocolError) {
    return new StreamingClientError(`${message}: ${error.message}`, {
      code: 'protocol',
      body: error.body,
    });
  }
  return toClientError(error);
}

function stringOf(value: unknown) {
  return typeof value === 'string' ? value : undefined;
}
