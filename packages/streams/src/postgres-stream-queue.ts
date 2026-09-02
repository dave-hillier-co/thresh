import type { Pool } from "pg";
import { raceSignal } from "@thresh/core/abort";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";
import { durationToMs, type Duration } from "@thresh/core/duration";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import type { AppendableQueue } from "@thresh/streams/pulling-stream-provider-core";
import type { QueueEntry } from "@thresh/streams/redis-stream-queue";
import { PostgresStreamCursorStore } from "@thresh/streams/postgres-stream-cursor-store";

// Table names are interpolated (Postgres cannot bind identifiers), so guard
// them against anything but a plain SQL identifier; all other inputs are bound.
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

// Concurrent silos can race `CREATE TABLE IF NOT EXISTS` and the migration
// below; the loser sees a duplicate error even though the object now exists,
// which is the desired state.
function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710" || code === "42701";
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
  private readonly serviceId: string;

  constructor(
    private readonly pool: Pool,
    tablePrefix: string,
    private readonly providerName: string,
    private readonly queueIdx: number,
    retainFor?: Duration,
    serviceId: string = DEFAULT_SERVICE_ID,
  ) {
    this.eventsTable = `${tablePrefix}_events`;
    if (!IDENTIFIER.test(this.eventsTable)) {
      throw new Error(`invalid table name: ${this.eventsTable}`);
    }
    this.serviceId = serviceId;
    this.cursors = new PostgresStreamCursorStore(pool, tablePrefix, serviceId);
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
           service_id TEXT NOT NULL,
           created_at TIMESTAMPTZ NOT NULL DEFAULT now()
         )`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
    // Must run before the index below: on a legacy (pre-#64) table the
    // `CREATE TABLE IF NOT EXISTS` above no-ops, leaving no `service_id`
    // column, and Postgres validates an index's columns before its
    // `IF NOT EXISTS` skip applies — so creating the index first raises
    // 42703 undefined_column on every restart against such a table.
    await this.addServiceIdColumn();
    try {
      await this.pool.query(
        `CREATE INDEX IF NOT EXISTS ${this.eventsTable}_provider_queue_id_idx
           ON ${this.eventsTable} (service_id, provider, queue_idx, id)`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
    await this.cursors.start();
  }

  /**
   * Bring an events table created before issue #64 up to the
   * service-partitioned shape, mirroring
   * `PostgresGrainStorage.addServiceIdColumn()` (#59). The events table has
   * a surrogate `id` primary key already, so this only adds the column and
   * backfills it — no primary key to swap. It also drops the old
   * `(provider, queue_idx, id)` lookup index, which shares its name with the
   * `service_id`-leading one `start()` (re)creates immediately after this
   * returns, so `CREATE INDEX IF NOT EXISTS` there doesn't silently keep the
   * stale definition.
   */
  private async addServiceIdColumn(): Promise<void> {
    const existing = await this.pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = current_schema() AND table_name = $1 AND column_name = 'service_id'`,
      [this.eventsTable],
    );
    if (existing.rowCount !== 0) return;
    try {
      await this.pool.query(
        `ALTER TABLE ${this.eventsTable}
           ADD COLUMN IF NOT EXISTS service_id text NOT NULL DEFAULT '${DEFAULT_SERVICE_ID}'`,
      );
      await this.pool.query(`ALTER TABLE ${this.eventsTable} ALTER COLUMN service_id DROP DEFAULT`);
      await this.pool.query(`DROP INDEX IF EXISTS ${this.eventsTable}_provider_queue_id_idx`);
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }

  /** Append an event for `streamKey`; returns its queue token. */
  async append(streamKey: string, event: unknown, signal?: AbortSignal): Promise<number> {
    const res = await raceSignal(
      this.pool.query<{ id: string }>(
        `INSERT INTO ${this.eventsTable} (provider, queue_idx, stream_key, payload, service_id)
         VALUES ($1, $2, $3, $4, $5)
         RETURNING id`,
        [this.providerName, this.queueIdx, streamKey, serializeValue(event), this.serviceId],
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
         WHERE provider = $1 AND queue_idx = $2 AND id > $3 AND service_id = $4
         ORDER BY id
         LIMIT $5`,
        [this.providerName, this.queueIdx, cursor, this.serviceId, count],
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

  /**
   * Unconditionally set the cursor, bypassing `commit`'s monotonic guard —
   * for an intentional rewind to an earlier checkpoint
   * (`RecoverableStreamDeliveryError`), never for ordinary advancement.
   * Deliberately does not trim: rows below the pre-rewind cursor may already
   * have been trimmed by a prior, further-ahead commit.
   */
  async seek(cursor: number, signal?: AbortSignal): Promise<void> {
    await this.cursors.seek(this.providerName, this.queueIdx, cursor, signal);
  }

  private async trim(cursor: number): Promise<void> {
    if (this.retainForMs === undefined) {
      await this.pool.query(
        `DELETE FROM ${this.eventsTable}
         WHERE provider = $1 AND queue_idx = $2 AND id <= $3 AND service_id = $4`,
        [this.providerName, this.queueIdx, cursor, this.serviceId],
      );
      return;
    }
    // Keep a replay window: only trim rows both delivered (below the cursor)
    // and older than `retainFor`.
    await this.pool.query(
      `DELETE FROM ${this.eventsTable}
       WHERE provider = $1 AND queue_idx = $2 AND id <= $3 AND service_id = $4
         AND created_at < now() - ($5 || ' milliseconds')::interval`,
      [this.providerName, this.queueIdx, cursor, this.serviceId, this.retainForMs],
    );
  }
}
