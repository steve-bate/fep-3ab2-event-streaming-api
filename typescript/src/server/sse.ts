/**
 * Server-Sent Events framing.
 *
 * ```
 * id: <event-id>
 * event: activitypub
 * data: <JSON-object>
 *
 * ```
 *
 * Frames are produced as wire-ready strings so an adapter only has to write and
 * flush them, with `node:http`, `Express`, `fetch` streams or anything else.
 */

import type { HttpHeaders } from './types.js';

export const SSE_CONTENT_TYPE = 'text/event-stream';

export const SSE_HEADERS: HttpHeaders = {
  'content-type': `${SSE_CONTENT_TYPE}; charset=utf-8`,
  'cache-control': 'no-cache, no-transform',
  connection: 'keep-alive',
  // Tell reverse proxies (nginx and friends) not to buffer the stream.
  'x-accel-buffering': 'no',
};

export interface SseFrame {
  /** Event id: the payload's `id`, or a server-generated value. */
  readonly id?: string;
  /** Event name; `activitypub` for entity frames, `heartbeat` for keep-alives. */
  readonly event?: string;
  /** Serialised as JSON unless it is already a string. */
  readonly data?: unknown;
  /** Reconnection delay hint, in milliseconds */
  readonly retryMs?: number;
  /** Free-form comment line, ignored by clients */
  readonly comment?: string;
}

export function formatSseFrame(frame: SseFrame): string {
  let out = '';

  if (frame.comment !== undefined) {
    for (const line of splitLines(frame.comment)) out += `: ${line}\n`;
  }
  if (frame.retryMs !== undefined) out += `retry: ${Math.max(0, Math.trunc(frame.retryMs))}\n`;
  if (frame.id !== undefined) out += `id: ${singleLine(frame.id)}\n`;
  if (frame.event !== undefined) out += `event: ${singleLine(frame.event)}\n`;
  if (frame.data !== undefined) {
    const text = typeof frame.data === 'string' ? frame.data : JSON.stringify(frame.data);
    for (const line of splitLines(text)) out += `data: ${line}\n`;
  }

  return `${out}\n`;
}

/** A keep-alive frame. The `data` field is optional per the spec but useful. */
export function heartbeatFrame(now: Date = new Date()): SseFrame {
  return { event: 'heartbeat', data: { ts: now.toISOString() } };
}

function splitLines(value: string) {
  return value.split(/\r\n|\r|\n/);
}

function singleLine(value: string) {
  return value.replace(/[\r\n]+/g, ' ');
}
