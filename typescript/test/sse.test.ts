import { describe, expect, it } from 'vitest';
import { formatSseFrame, heartbeatFrame, SSE_HEADERS } from '@fep-3ab2/streaming/server';
import { SseParser, parseSseText } from '@fep-3ab2/streaming/testing';

describe('formatSseFrame', () => {
  it('writes the FEP-3ab2 frame shape', () => {
    const frame = formatSseFrame({
      id: 'https://example.org/activities/1',
      event: 'activitypub',
      data: { type: 'Create' },
    });

    expect(frame).toBe(
      'id: https://example.org/activities/1\nevent: activitypub\ndata: {"type":"Create"}\n\n',
    );
  });

  it('splits multi-line data into several data lines', () => {
    const frame = formatSseFrame({ event: 'note', data: 'line one\nline two' });
    expect(frame).toBe('event: note\ndata: line one\ndata: line two\n\n');
  });

  it('never lets an id or event name break the framing', () => {
    const frame = formatSseFrame({ id: 'a\nb', event: 'x\r\ny', data: 'ok' });
    expect(frame).toBe('id: a b\nevent: x y\ndata: ok\n\n');
  });

  it('omits optional fields', () => {
    expect(formatSseFrame({ event: 'heartbeat' })).toBe('event: heartbeat\n\n');
  });

  it('writes heartbeat events with a timestamp', () => {
    const frame = heartbeatFrame(new Date('2026-03-10T12:00:00Z'));
    expect(frame.event).toBe('heartbeat');
    expect(formatSseFrame(frame)).toContain('data: {"ts":"2026-03-10T12:00:00.000Z"}');
  });

  it('carries the headers an SSE response needs', () => {
    expect(SSE_HEADERS['content-type']).toMatch(/^text\/event-stream/);
    expect(SSE_HEADERS['cache-control']).toContain('no-cache');
    expect(SSE_HEADERS['x-accel-buffering']).toBe('no');
  });
});

describe('SseParser', () => {
  it('round-trips a frame', () => {
    const events = parseSseText(formatSseFrame({ id: '1', event: 'activitypub', data: { a: 1 } }));
    expect(events).toEqual([{ id: '1', event: 'activitypub', data: '{"a":1}' }]);
  });

  it('handles chunk boundaries in the middle of a field', () => {
    const text = formatSseFrame({ id: 'abc', event: 'activitypub', data: { hello: 'world' } });
    const parser = new SseParser();
    const events = [];
    for (const character of text) events.push(...parser.push(character));
    expect(events).toHaveLength(1);
    expect(events[0]?.data).toBe('{"hello":"world"}');
  });

  it('is not confused by comment lines', () => {
    expect(parseSseText(': keep-alive\n\nevent: heartbeat\ndata: {}\n\n')).toEqual([
      { id: undefined, event: 'heartbeat', data: '{}' },
    ]);
  });

  it('keeps the last id across events, as EventSource does', () => {
    const events = parseSseText('id: 7\nevent: a\ndata: 1\n\nevent: b\ndata: 2\n\n');
    expect(events.map((event) => event.id)).toEqual(['7', '7']);
  });
});
