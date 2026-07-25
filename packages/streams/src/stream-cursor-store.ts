/**
 * Durable per-`(provider, queueIdx)` cursor storage — the seam Phase 1
 * (Postgres) and Phase 2 (Kafka, docs/stream-backings-postgres-kafka.md)
 * share instead of duplicating cursor SQL/Redis-key logic. A backing whose
 * physical queue is itself the source of truth for its events (Postgres'
 * `<prefix>_events` table, a Redis Stream) can keep the cursor beside the
 * events; a backing whose transport has no metadata surface of its own
 * (Kafka) composes one of these instead — Orleans EventHubs' Azure Table
 * checkpoint store is the same shape.
 */
export interface StreamCursorStore {
  /** The committed cursor for `(provider, queueIdx)` (0 if never committed). */
  getCursor(provider: string, queueIdx: number, signal?: AbortSignal): Promise<number>;
  commit(provider: string, queueIdx: number, cursor: number, signal?: AbortSignal): Promise<void>;
}

/** In-memory `StreamCursorStore` — the default for tests and single-process silos. */
export class MemoryStreamCursorStore implements StreamCursorStore {
  private readonly cursors = new Map<string, number>();

  private key(provider: string, queueIdx: number): string {
    return `${provider}/${queueIdx}`;
  }

  async getCursor(provider: string, queueIdx: number): Promise<number> {
    return this.cursors.get(this.key(provider, queueIdx)) ?? 0;
  }

  async commit(provider: string, queueIdx: number, cursor: number): Promise<void> {
    this.cursors.set(this.key(provider, queueIdx), cursor);
  }
}
