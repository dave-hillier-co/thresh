import { randomUUID } from "node:crypto";
import type { Pool } from "pg";
import type { GrainId } from "@thresh/core/grain-id";
import type { ReminderEntry, ReminderRegistration, ReminderTable } from "@thresh/core/reminder";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import type { ReminderData } from "./reminder-data";

export interface PostgresReminderTableOptions {
  /** Table holding the reminder rows (defaults to `"thresh_reminders"`). */
  tableName?: string;
}

// Table/index names are interpolated (Postgres cannot bind identifiers), so
// guard them against anything but a plain SQL identifier; all values are bound.
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07";
}

/**
 * Durable, cluster-shared reminder table backed by Postgres, mirroring the
 * Redis table. Each reminder is one row keyed by `(grain_id, name)`; a `hash`
 * column (the grain's uniform hash code) is indexed so `readRange` ownership —
 * including wrap-around — is a server-side range query, and `readForGrain` is a
 * `grain_id` lookup. Upsert is unconditional with a fresh etag; remove is an
 * etag compare-and-set. Interchangeable with `MemoryReminderTable`. `start()`
 * creates the table (idempotent); the host runs it on start.
 */
export class PostgresReminderTable implements ReminderTable {
  private readonly table: string;

  constructor(
    private readonly pool: Pool,
    options: PostgresReminderTableOptions = {},
  ) {
    this.table = options.tableName ?? "thresh_reminders";
    if (!IDENTIFIER.test(this.table)) throw new Error(`invalid table name: ${this.table}`);
  }

  async start(): Promise<void> {
    try {
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS ${this.table} (
           grain_id text NOT NULL,
           name text NOT NULL,
           hash bigint NOT NULL,
           data text NOT NULL,
           etag text NOT NULL,
           PRIMARY KEY (grain_id, name)
         )`,
      );
      await this.pool.query(
        `CREATE INDEX IF NOT EXISTS ${this.table}_hash_idx ON ${this.table} (hash)`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }

  async upsert(registration: ReminderRegistration): Promise<string> {
    const { grainId, name } = registration;
    const etag = randomUUID();
    const data: ReminderData = {
      grainId,
      name,
      startAt: registration.startAt,
      period: registration.period,
    };
    await this.pool.query(
      `INSERT INTO ${this.table} (grain_id, name, hash, data, etag)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (grain_id, name) DO UPDATE
         SET hash = EXCLUDED.hash, data = EXCLUDED.data, etag = EXCLUDED.etag`,
      // bigint is bound as a string so values above 2^31 round-trip safely.
      [grainId.toString(), name, String(grainId.getUniformHashCode()), serializeValue(data), etag],
    );
    return etag;
  }

  async remove(grainId: GrainId, name: string, etag: string): Promise<boolean> {
    const res = await this.pool.query(
      `DELETE FROM ${this.table} WHERE grain_id = $1 AND name = $2 AND etag = $3`,
      [grainId.toString(), name, etag],
    );
    return res.rowCount === 1;
  }

  async read(grainId: GrainId, name: string): Promise<ReminderEntry | undefined> {
    const res = await this.pool.query<{ data: string; etag: string }>(
      `SELECT data, etag FROM ${this.table} WHERE grain_id = $1 AND name = $2`,
      [grainId.toString(), name],
    );
    return this.toEntry(res.rows[0]);
  }

  async readForGrain(grainId: GrainId): Promise<ReminderEntry[]> {
    const res = await this.pool.query<{ data: string; etag: string }>(
      `SELECT data, etag FROM ${this.table} WHERE grain_id = $1`,
      [grainId.toString()],
    );
    return res.rows.map((r) => this.toEntry(r)!);
  }

  async readRange(hashBegin: number, hashEnd: number): Promise<ReminderEntry[]> {
    // Half-open mirror of MemoryReminderTable.inRange, server-side: non-wrap
    // (begin <= end) is [begin, end); wrap (begin > end) is hash >= begin OR
    // hash < end. bigint bounds are bound as strings.
    const res = await this.pool.query<{ data: string; etag: string }>(
      `SELECT data, etag FROM ${this.table}
       WHERE CASE WHEN $1::bigint <= $2::bigint
         THEN hash >= $1::bigint AND hash < $2::bigint
         ELSE hash >= $1::bigint OR  hash < $2::bigint
       END`,
      [String(hashBegin), String(hashEnd)],
    );
    return res.rows.map((r) => this.toEntry(r)!);
  }

  private toEntry(row: { data: string; etag: string } | undefined): ReminderEntry | undefined {
    if (row === undefined) return undefined;
    const data = deserializeValue<ReminderData>(row.data);
    return { ...data, etag: row.etag };
  }
}
