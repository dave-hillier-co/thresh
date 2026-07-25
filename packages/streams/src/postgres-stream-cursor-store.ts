import type { Pool } from "pg";
import { raceSignal } from "@tsva/core/abort";
import type { StreamCursorStore } from "@tsva/streams/stream-cursor-store";

const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710";
}

/**
 * `StreamCursorStore` backed by `<prefix>_cursors`, the same table shape
 * `PostgresStreamQueue` provisions for Phase 1 — `PostgresStreamQueue`
 * composes this rather than duplicating the cursor SQL, and it is also the
 * metadata store a Kafka backing (Phase 2) can plug in for cursor durability
 * when it wants Postgres rather than Redis for that role.
 */
export class PostgresStreamCursorStore implements StreamCursorStore {
  private readonly table: string;

  constructor(
    private readonly pool: Pool,
    tablePrefix: string,
  ) {
    this.table = `${tablePrefix}_cursors`;
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
           PRIMARY KEY (provider, queue_idx)
         )`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }

  async getCursor(provider: string, queueIdx: number, signal?: AbortSignal): Promise<number> {
    const res = await raceSignal(
      this.pool.query<{ cursor: string }>(
        `SELECT cursor FROM ${this.table} WHERE provider = $1 AND queue_idx = $2`,
        [provider, queueIdx],
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
        `INSERT INTO ${this.table} (provider, queue_idx, cursor)
         VALUES ($1, $2, $3)
         ON CONFLICT (provider, queue_idx) DO UPDATE SET cursor = EXCLUDED.cursor`,
        [provider, queueIdx, cursor],
      ),
      signal,
    );
  }
}
