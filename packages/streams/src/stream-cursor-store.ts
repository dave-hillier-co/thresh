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
  /**
   * Advance the committed cursor after at-least-once delivery — monotonic:
   * only ever advances. A stale commit racing in from a de-owned pulling
   * agent during ownership handoff must not rewind a newer commit the new
   * owner already made (which would redeliver a whole batch).
   */
  commit(provider: string, queueIdx: number, cursor: number, signal?: AbortSignal): Promise<void>;
  /**
   * Unconditionally set the committed cursor, bypassing `commit`'s monotonic
   * guard. Used only for an intentional rewind to an earlier checkpoint
   * (`RecoverableStreamDeliveryError`).
   */
  seek(provider: string, queueIdx: number, cursor: number, signal?: AbortSignal): Promise<void>;
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

  // Monotonic: a stale commit racing in from a de-owned agent after the new
  // owner has already committed further ahead must not rewind the cursor and
  // redeliver a whole batch (ownership-handoff regression).
  async commit(provider: string, queueIdx: number, cursor: number): Promise<void> {
    const key = this.key(provider, queueIdx);
    const current = this.cursors.get(key) ?? 0;
    if (cursor > current) this.cursors.set(key, cursor);
  }

  async seek(provider: string, queueIdx: number, cursor: number): Promise<void> {
    this.cursors.set(this.key(provider, queueIdx), cursor);
  }
}
