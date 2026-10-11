import type { Pool } from "pg";
import { raceSignal } from "@thresh/core/abort";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";
import type { StreamCursorStore } from "@thresh/streams/stream-cursor-store";

const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710" || code === "42701";
}

/**
 * `StreamCursorStore` backed by `<prefix>_cursors`, the same table shape
 * `PostgresStreamQueue` provisions for Phase 1 — `PostgresStreamQueue`
 * composes this rather than duplicating the cursor SQL, and it is also the
 * metadata store a Kafka backing (Phase 2) can plug in for cursor durability
 * when it wants Postgres rather than Redis for that role. Partitioned by
 * `service_id` (issue #64, following #59's `PostgresGrainStorage` pattern) so
 * several services sharing one table don't reset or cross-read each other's
 * committed cursors.
 */
export class PostgresStreamCursorStore implements StreamCursorStore {
  private readonly table: string;
  private readonly serviceId: string;

  constructor(
    private readonly pool: Pool,
    tablePrefix: string,
    serviceId: string = DEFAULT_SERVICE_ID,
  ) {
    this.table = `${tablePrefix}_cursors`;
    this.serviceId = serviceId;
    if (!IDENTIFIER.test(this.table)) throw new Error(`invalid table name: ${this.table}`);
  }

  /** Idempotently provisions the cursors table. */
  async start(): Promise<void> {
    try {
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS ${this.table} (
           provider TEXT NOT NULL,
           queue_idx INT NOT NULL,
           cursor BIGINT NOT NULL,
           service_id TEXT NOT NULL,
           PRIMARY KEY (service_id, provider, queue_idx)
         )`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }

  async getCursor(provider: string, queueIdx: number, signal?: AbortSignal): Promise<number> {
    const res = await raceSignal(
      this.pool.query<{ cursor: string }>(
        `SELECT cursor FROM ${this.table} WHERE provider = $1 AND queue_idx = $2 AND service_id = $3`,
        [provider, queueIdx, this.serviceId],
      ),
      signal,
    );
    return res.rows[0] === undefined ? 0 : Number(res.rows[0].cursor);
  }

  // Monotonic: the WHERE guard on the upsert only lets `cursor` advance, so a
  // stale commit racing in from a de-owned pulling agent (ownership handoff)
  // can never rewind a newer commit from the new owner and cause a whole
  // batch to be redelivered.
  async commit(
    provider: string,
    queueIdx: number,
    cursor: number,
    signal?: AbortSignal,
  ): Promise<void> {
    await raceSignal(
      this.pool.query(
        `INSERT INTO ${this.table} (provider, queue_idx, cursor, service_id)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (service_id, provider, queue_idx) DO UPDATE
           SET cursor = EXCLUDED.cursor
           WHERE ${this.table}.cursor < EXCLUDED.cursor`,
        [provider, queueIdx, cursor, this.serviceId],
      ),
      signal,
    );
  }

  /**
   * Unconditionally set the cursor, bypassing `commit`'s monotonic guard —
   * for an intentional rewind to an earlier checkpoint
   * (`RecoverableStreamDeliveryError`), never for ordinary advancement.
   */
  async seek(
    provider: string,
    queueIdx: number,
    cursor: number,
    signal?: AbortSignal,
  ): Promise<void> {
    await raceSignal(
      this.pool.query(
        `INSERT INTO ${this.table} (provider, queue_idx, cursor, service_id)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (service_id, provider, queue_idx) DO UPDATE SET cursor = EXCLUDED.cursor`,
        [provider, queueIdx, cursor, this.serviceId],
      ),
      signal,
    );
  }
}
