import type { Pool } from "pg";
import { raceSignal } from "@tsva/core/abort";
import { durationToMs, type Duration } from "@tsva/core/duration";
import { deserializeValue, serializeValue } from "@tsva/core/value-codec";
import type { AppendableQueue } from "@tsva/streams/pulling-stream-provider-core";
import type { QueueEntry } from "@tsva/streams/redis-stream-queue";
import { PostgresStreamCursorStore } from "@tsva/streams/postgres-stream-cursor-store";

// Table names are interpolated (Postgres cannot bind identifiers), so guard
// them against anything but a plain SQL identifier; all other inputs are bound.
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

// Concurrent silos can race `CREATE TABLE IF NOT EXISTS`; the loser sees a
// duplicate error even though the table now exists, which is the desired state.
function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710";
}

/**
 * One physical queue backed by two Postgres tables shared across every queue
 * index of a provider (`<prefix>_events`, `<prefix>_cursors`), partitioned by
 * `(provider, queue_idx)` — the same multiplexing shape `RedisStreamQueue`
 * gives one Redis Stream key per queue index. `append` inserts and returns
 * the row's `BIGSERIAL` id: the global sequence is strictly monotonic, so it
 * is a valid per-queue token even though it is shared across queues (the
 * `QueueEntry.token` contract only requires monotonic-within-queue).
 */
export class PostgresStreamQueue implements AppendableQueue {
  private readonly eventsTable: string;
  private readonly cursors: PostgresStreamCursorStore;
  private readonly retainForMs: number | undefined;

  constructor(
    private readonly pool: Pool,
    tablePrefix: string,
    private readonly providerName: string,
    private readonly queueIdx: number,
    retainFor?: Duration,
  ) {
    this.eventsTable = `${tablePrefix}_events`;
    if (!IDENTIFIER.test(this.eventsTable)) {
      throw new Error(`invalid table name: ${this.eventsTable}`);
    }
    this.cursors = new PostgresStreamCursorStore(pool, tablePrefix);
    this.retainForMs = retainFor === undefined ? undefined : durationToMs(retainFor);
  }

  /** Idempotently provisions the shared events/cursors tables and their index. */
  async start(): Promise<void> {
    try {
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS ${this.eventsTable} (
           id BIGSERIAL PRIMARY KEY,
           provider TEXT NOT NULL,
           queue_idx INT NOT NULL,
           stream_key TEXT NOT NULL,
           payload TEXT NOT NULL,
           created_at TIMESTAMPTZ NOT NULL DEFAULT now()
         )`,
      );
      await this.pool.query(
        `CREATE INDEX IF NOT EXISTS ${this.eventsTable}_provider_queue_id_idx
           ON ${this.eventsTable} (provider, queue_idx, id)`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
    await this.cursors.start();
  }

  /** Append an event for `streamKey`; returns its queue token. */
  async append(streamKey: string, event: unknown, signal?: AbortSignal): Promise<number> {
    const res = await raceSignal(
      this.pool.query<{ id: string }>(
        `INSERT INTO ${this.eventsTable} (provider, queue_idx, stream_key, payload)
         VALUES ($1, $2, $3, $4)
         RETURNING id`,
        [this.providerName, this.queueIdx, streamKey, serializeValue(event)],
      ),
      signal,
    );
    return Number(res.rows[0]!.id);
  }

  /** Entries with token strictly greater than `cursor`, in order. */
  async readAfter(cursor: number, count = 128, signal?: AbortSignal): Promise<QueueEntry[]> {
    const res = await raceSignal(
      this.pool.query<{ id: string; stream_key: string; payload: string }>(
        `SELECT id, stream_key, payload FROM ${this.eventsTable}
         WHERE provider = $1 AND queue_idx = $2 AND id > $3
         ORDER BY id
         LIMIT $4`,
        [this.providerName, this.queueIdx, cursor, count],
      ),
      signal,
    );
    return res.rows.map((row) => ({
      token: Number(row.id),
      streamKey: row.stream_key,
      event: deserializeValue(row.payload),
    }));
  }

  /** The committed cursor (0 if the queue has never been read). */
  async getCursor(signal?: AbortSignal): Promise<number> {
    return this.cursors.getCursor(this.providerName, this.queueIdx, signal);
  }

  async commit(cursor: number, signal?: AbortSignal): Promise<void> {
    await this.cursors.commit(this.providerName, this.queueIdx, cursor, signal);
    // Opportunistic trim: delivered rows below the committed cursor are dead
    // weight. Never fails the commit — a trim error is swallowed (logged to
    // stderr) so a transient issue here can't turn a successful delivery into
    // a failed one.
    try {
      await this.trim(cursor);
    } catch (err) {
      console.error(
        `postgres stream queue: trim failed for ${this.providerName}/${this.queueIdx}`,
        err,
      );
    }
  }

  private async trim(cursor: number): Promise<void> {
    if (this.retainForMs === undefined) {
      await this.pool.query(
        `DELETE FROM ${this.eventsTable} WHERE provider = $1 AND queue_idx = $2 AND id <= $3`,
        [this.providerName, this.queueIdx, cursor],
      );
      return;
    }
    // Keep a replay window: only trim rows both delivered (below the cursor)
    // and older than `retainFor`.
    await this.pool.query(
      `DELETE FROM ${this.eventsTable}
       WHERE provider = $1 AND queue_idx = $2 AND id <= $3
         AND created_at < now() - ($4 || ' milliseconds')::interval`,
      [this.providerName, this.queueIdx, cursor, this.retainForMs],
    );
  }
}
