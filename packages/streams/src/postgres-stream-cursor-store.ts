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
    await this.addServiceIdColumn();
  }

  /**
   * Bring a table created before issue #64 up to the service-partitioned
   * shape, mirroring `PostgresGrainStorage.addServiceIdColumn()` (#59). The
   * cursor value itself is what gets backfilled here — a mismatch between
   * this migration and the default reader would reset a committed cursor to
   * 0, silently redelivering (or, with eager trim, skipping) a whole queue's
   * history.
   */
  private async addServiceIdColumn(): Promise<void> {
    const existing = await this.pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = current_schema() AND table_name = $1 AND column_name = 'service_id'`,
      [this.table],
    );
    if (existing.rowCount !== 0) return;
    try {
      await this.pool.query(
        `ALTER TABLE ${this.table}
           ADD COLUMN IF NOT EXISTS service_id text NOT NULL DEFAULT '${DEFAULT_SERVICE_ID}'`,
      );
      await this.pool.query(`ALTER TABLE ${this.table} ALTER COLUMN service_id DROP DEFAULT`);
      const pk = await this.pool.query<{ conname: string }>(
        `SELECT conname FROM pg_constraint WHERE contype = 'p' AND conrelid = to_regclass($1)`,
        [this.table],
      );
      const oldKey = pk.rows[0]?.conname;
      if (oldKey !== undefined) {
        await this.pool.query(`ALTER TABLE ${this.table} DROP CONSTRAINT "${oldKey}"`);
      }
      await this.pool.query(
        `ALTER TABLE ${this.table} ADD PRIMARY KEY (service_id, provider, queue_idx)`,
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
         ON CONFLICT (service_id, provider, queue_idx) DO UPDATE SET cursor = EXCLUDED.cursor`,
        [provider, queueIdx, cursor, this.serviceId],
      ),
      signal,
    );
  }
}
