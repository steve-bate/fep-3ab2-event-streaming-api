/* eslint-disable @typescript-eslint/require-await -- the interface is async so
   a database-backed store can implement it without changing callers. */
/**
 * Per-principal topic subscriptions.
 *
 * Subscriptions belong to the principal, not to a ticket or a stream, so they
 * survive ticket renewal and reconnect. Adding an existing topic and removing a
 * missing one are both no-ops, as the spec requires.
 */

export interface SubscriptionStore {
  /** Topics subscribed by the principal, sorted, without duplicates. */
  list(principal: string): Promise<string[]>;
  /** Adds topics idempotently; returns the full set afterwards. */
  add(principal: string, topics: readonly string[]): Promise<string[]>;
  /** Removes a topic idempotently; returns the full set afterwards. */
  remove(principal: string, topic: string): Promise<string[]>;
  /** Principals that currently hold at least one subscription. */
  principals(): Promise<string[]>;
  forget(principal: string): Promise<void>;
}

export class InMemorySubscriptionStore implements SubscriptionStore {
  readonly #byPrincipal = new Map<string, Set<string>>();

  async list(principal: string): Promise<string[]> {
    return sortedTopics(this.#byPrincipal.get(principal));
  }

  async add(principal: string, topics: readonly string[]): Promise<string[]> {
    if (topics.length > 0) {
      const set = this.#byPrincipal.get(principal) ?? new Set<string>();
      for (const topic of topics) set.add(topic);
      this.#byPrincipal.set(principal, set);
    }
    return sortedTopics(this.#byPrincipal.get(principal));
  }

  async remove(principal: string, topic: string): Promise<string[]> {
    const set = this.#byPrincipal.get(principal);
    if (set === undefined) return [];
    set.delete(topic);
    if (set.size === 0) this.#byPrincipal.delete(principal);
    return sortedTopics(set);
  }

  async principals(): Promise<string[]> {
    return [...this.#byPrincipal.keys()].sort();
  }

  async forget(principal: string): Promise<void> {
    this.#byPrincipal.delete(principal);
  }
}

function sortedTopics(set: Set<string> | undefined) {
  return set === undefined ? [] : [...set].sort();
}
