/**
 * A W3C EventSource-compatible parser, used by the in-process loopback and by
 * tests that need to inspect the exact bytes the server produced.
 */

export interface ParsedSseEvent {
  readonly id: string | undefined;
  readonly event: string | undefined;
  readonly data: string;
}

/** Incremental parser: feed chunks, get the events they completed. */
export class SseParser {
  #buffer = '';
  #id: string | undefined;
  #event: string | undefined;
  #data: string[] = [];

  push(chunk: string): ParsedSseEvent[] {
    this.#buffer += chunk;
    const events: ParsedSseEvent[] = [];

    for (;;) {
      const breakIndex = this.#buffer.search(/\r\n|\r|\n/);
      if (breakIndex < 0) break;

      const match = /^(\r\n|\r|\n)/.exec(this.#buffer.slice(breakIndex));
      const separator = match?.[1] ?? '\n';
      const line = this.#buffer.slice(0, breakIndex);
      this.#buffer = this.#buffer.slice(breakIndex + separator.length);

      const dispatched = this.#line(line);
      if (dispatched !== undefined) events.push(dispatched);
    }

    return events;
  }

  /** Flushes a trailing complete frame (a stream may end without a blank line). */
  flush(): ParsedSseEvent[] {
    if (this.#buffer.length > 0) {
      const line = this.#buffer;
      this.#buffer = '';
      const dispatched = this.#line(line);
      return dispatched === undefined ? [] : [dispatched];
    }
    return [];
  }

  #line(line: string): ParsedSseEvent | undefined {
    if (line.length === 0) return this.#dispatch();

    if (line.startsWith(':')) return undefined; // comment / heartbeat padding

    const separator = line.indexOf(':');
    const field = separator < 0 ? line : line.slice(0, separator);
    let value = separator < 0 ? '' : line.slice(separator + 1);
    if (value.startsWith(' ')) value = value.slice(1);

    switch (field) {
      case 'id':
        this.#id = value;
        return undefined;
      case 'event':
        this.#event = value;
        return undefined;
      case 'data':
        this.#data.push(value);
        return undefined;
      default:
        return undefined;
    }
  }

  #dispatch(): ParsedSseEvent | undefined {
    if (this.#data.length === 0 && this.#event === undefined) {
      this.#id = undefined;
      return undefined;
    }

    const event: ParsedSseEvent = {
      id: this.#id,
      event: this.#event,
      data: this.#data.join('\n'),
    };

    // `id` persists across events per the EventSource spec; `event` and data do not.
    this.#event = undefined;
    this.#data = [];
    return event;
  }
}

/** Parses a complete stream body. */
export function parseSseText(text: string): ParsedSseEvent[] {
  const parser = new SseParser();
  return [...parser.push(text), ...parser.flush()];
}
