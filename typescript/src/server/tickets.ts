/**
 * Ticket storage.
 *
 * A ticket authorises exactly one stream. Two independent facts are tracked:
 * whether the ticket is still *valid* (unexpired, not revoked) and whether it has
 * been *consumed* by opening a stream. Subscription management keeps working
 * with a consumed ticket; only `openStream` insists on a fresh one.
 *
 * Records are keyed by the SHA-256 of the ticket, so a dump of the store never
 * contains a usable credential, in line with the spec's rule that ticket values
 * must not appear in logs. Any store shared between processes must implement its
 * own atomic `consume`; the in-memory guard below covers one process.
 */

import { generateTicket, sha256Hex } from './crypto.js';

export interface TicketRecord {
  /** Actor URI of the authenticated principal the ticket was issued to. */
  readonly principal: string;
  readonly issuedAt: Date;
  readonly expiresAt: Date;
  /** Browser origin that requested the session, for audit */
  readonly origin?: string;
  /** Set once a stream has been opened with this ticket */
  readonly consumedAt?: Date;
}

export interface TicketStore {
  create(ticket: string, record: TicketRecord): Promise<void>;
  /**
   * Reads a ticket that is still valid. Returns `null` for unknown, expired or
   * revoked tickets; a *consumed* ticket is still returned (see the module note).
   */
  read(ticket: string): Promise<TicketRecord | null>;
  /**
   * Atomically marks the ticket used and returns its record, or `null` when it is
   * unknown, expired or already consumed. That is the "one stream per ticket"
   * guarantee.
   */
  consume(ticket: string): Promise<TicketRecord | null>;
  revoke(ticket: string): Promise<void>;
  /** Drops expired records; returns how many were removed. */
  prune(now?: Date): Promise<number>;
}

export interface InMemoryTicketStoreOptions {
  /** Override the key derivation, e.g. to use a KMS-backed HMAC. */
  readonly hashTicket?: (ticket: string) => Promise<string>;
  /** Clock, for tests */
  readonly now?: () => Date;
}

export class InMemoryTicketStore implements TicketStore {
  readonly #records = new Map<string, TicketRecord>();
  readonly #inFlight = new Set<string>();
  readonly #hash: (ticket: string) => Promise<string>;
  readonly #now: () => Date;

  constructor(options: InMemoryTicketStoreOptions = {}) {
    this.#hash = options.hashTicket ?? sha256Hex;
    this.#now = options.now ?? (() => new Date());
  }

  async create(ticket: string, record: TicketRecord): Promise<void> {
    this.#records.set(await this.#hash(ticket), record);
  }

  async read(ticket: string): Promise<TicketRecord | null> {
    const key = await this.#hash(ticket);
    const record = this.#records.get(key);
    if (record === undefined) return null;
    if (hasExpired(record, this.#now())) {
      this.#records.delete(key);
      return null;
    }
    return record;
  }

  async consume(ticket: string): Promise<TicketRecord | null> {
    // Serialise concurrent attempts on the same ticket: the second one loses.
    if (this.#inFlight.has(ticket)) return null;
    this.#inFlight.add(ticket);

    try {
      const key = await this.#hash(ticket);
      const record = this.#records.get(key);
      if (record === undefined) return null;
      if (record.consumedAt !== undefined) return null;
      if (hasExpired(record, this.#now())) {
        this.#records.delete(key);
        return null;
      }
      this.#records.set(key, { ...record, consumedAt: this.#now() });
      return record;
    } finally {
      this.#inFlight.delete(ticket);
    }
  }

  async revoke(ticket: string): Promise<void> {
    this.#records.delete(await this.#hash(ticket));
  }

  // eslint-disable-next-line @typescript-eslint/require-await -- async for the interface
  async prune(now: Date = this.#now()): Promise<number> {
    let removed = 0;
    for (const [key, record] of this.#records) {
      if (hasExpired(record, now)) {
        this.#records.delete(key);
        removed += 1;
      }
    }
    return removed;
  }
}

export function hasExpired(record: TicketRecord, now: Date = new Date()): boolean {
  return record.expiresAt.getTime() <= now.getTime();
}

export interface IssueTicketOptions {
  readonly principal: string;
  readonly ttlSeconds: number;
  readonly origin?: string;
  readonly now?: () => Date;
  readonly generate?: () => string;
}

export function issueTicket(options: IssueTicketOptions): { ticket: string; record: TicketRecord } {
  const now = (options.now ?? (() => new Date()))();
  const ticket = (options.generate ?? generateTicket)();
  const record: TicketRecord = {
    principal: options.principal,
    issuedAt: now,
    expiresAt: new Date(now.getTime() + options.ttlSeconds * 1000),
    ...(options.origin === undefined ? {} : { origin: options.origin }),
  };
  return { ticket, record };
}
