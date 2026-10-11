import { randomUUID } from "node:crypto";
import type { Pool } from "pg";
import type { GrainId } from "@thresh/core/grain-id";
import type { ReminderEntry, ReminderRegistration, ReminderTable } from "@thresh/core/reminder";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";
import type { ReminderData } from "./reminder-data";

export interface PostgresReminderTableOptions {
  /** Table holding the reminder rows (defaults to `"thresh_reminders"`). */
  tableName?: string;
  /**
   * Logical service identity this provider's rows belong to (Orleans'
   * `ClusterOptions.ServiceId`). Two clusters pointed at the same table stay
   * partitioned by it. Defaults to `"default"`, Orleans' own
   * `ClusterOptions.DefaultServiceId`; `SiloBuilder` threads the silo's
   * `serviceId ?? DEFAULT_SERVICE_ID`.
   */
  serviceId?: string;
}

// Table/index names are interpolated (Postgres cannot bind identifiers), so
// guard them against anything but a plain SQL identifier; all values are bound.
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

// Concurrent silos can race `CREATE TABLE IF NOT EXISTS`; the loser sees a duplicate error even though the object now exists,
// which is the desired state. 42P07 duplicate_table, 42710 duplicate_object
// (a constraint), 42701 duplicate_column, 23505 unique_violation.
function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710" || code === "42701";
}

/**
 * Durable, cluster-shared reminder table backed by Postgres, mirroring the
 * Redis table. Each reminder is one row keyed by `(service_id, grain_id,
 * name)`; a `hash` column (the grain's uniform hash code) is indexed so
 * `readRange` ownership — including wrap-around — is a server-side range
 * query, and `readForGrain` is a `grain_id` lookup. Upsert is unconditional
 * with a fresh etag; remove is an etag compare-and-set. Interchangeable with
 * `MemoryReminderTable`. `start()` creates the table (idempotent); the host
 * runs it on start.
 */
export class PostgresReminderTable implements ReminderTable {
  private readonly table: string;
  private readonly serviceId: string;

  constructor(
    private readonly pool: Pool,
    options: PostgresReminderTableOptions = {},
  ) {
    this.table = options.tableName ?? "thresh_reminders";
    this.serviceId = options.serviceId ?? DEFAULT_SERVICE_ID;
    if (!IDENTIFIER.test(this.table)) throw new Error(`invalid table name: ${this.table}`);
  }

  async start(): Promise<void> {
    try {
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS ${this.table} (
           grain_id text NOT NULL,
           name text NOT NULL,
           service_id text NOT NULL,
           hash bigint NOT NULL,
           data text NOT NULL,
           etag text NOT NULL,
           last_fired_at bigint,
           PRIMARY KEY (service_id, grain_id, name)
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
      `INSERT INTO ${this.table} (grain_id, name, service_id, hash, data, etag)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (service_id, grain_id, name) DO UPDATE
         SET hash = EXCLUDED.hash, data = EXCLUDED.data, etag = EXCLUDED.etag,
             last_fired_at = NULL`,
      // bigint is bound as a string so values above 2^31 round-trip safely.
      [
        grainId.toString(),
        name,
        this.serviceId,
        String(grainId.getUniformHashCode()),
        serializeValue(data),
        etag,
      ],
    );
    return etag;
  }

  async remove(grainId: GrainId, name: string, etag: string): Promise<boolean> {
    const res = await this.pool.query(
      `DELETE FROM ${this.table}
       WHERE service_id = $1 AND grain_id = $2 AND name = $3 AND etag = $4`,
      [this.serviceId, grainId.toString(), name, etag],
    );
    return res.rowCount === 1;
  }

  async recordFired(
    grainId: GrainId,
    name: string,
    etag: string,
    firedAt: Date,
  ): Promise<string | undefined> {
    const res = await this.pool.query(
      `UPDATE ${this.table} SET last_fired_at = $5
       WHERE service_id = $1 AND grain_id = $2 AND name = $3 AND etag = $4`,
      [this.serviceId, grainId.toString(), name, etag, firedAt.getTime()],
    );
    return res.rowCount === 1 ? etag : undefined;
  }

  async read(grainId: GrainId, name: string): Promise<ReminderEntry | undefined> {
    const res = await this.pool.query<ReminderRow>(
      `SELECT data, etag, last_fired_at FROM ${this.table}
       WHERE service_id = $1 AND grain_id = $2 AND name = $3`,
      [this.serviceId, grainId.toString(), name],
    );
    return this.toEntry(res.rows[0]);
  }

  async readForGrain(grainId: GrainId): Promise<ReminderEntry[]> {
    const res = await this.pool.query<ReminderRow>(
      `SELECT data, etag, last_fired_at FROM ${this.table} WHERE service_id = $1 AND grain_id = $2`,
      [this.serviceId, grainId.toString()],
    );
    return res.rows.map((r) => this.toEntry(r)!);
  }

  async readRange(hashBegin: number, hashEnd: number): Promise<ReminderEntry[]> {
    // Half-open mirror of MemoryReminderTable.inRange, server-side: non-wrap
    // (begin <= end) is [begin, end); wrap (begin > end) is hash >= begin OR
    // hash < end. bigint bounds are bound as strings.
    const res = await this.pool.query<ReminderRow>(
      `SELECT data, etag, last_fired_at FROM ${this.table}
       WHERE service_id = $3 AND CASE WHEN $1::bigint <= $2::bigint
         THEN hash >= $1::bigint AND hash < $2::bigint
         ELSE hash >= $1::bigint OR  hash < $2::bigint
       END`,
      [String(hashBegin), String(hashEnd), this.serviceId],
    );
    return res.rows.map((r) => this.toEntry(r)!);
  }

  private toEntry(row: ReminderRow | undefined): ReminderEntry | undefined {
    if (row === undefined) return undefined;
    const data = deserializeValue<ReminderData>(row.data);
    const lastFiredAt =
      row.last_fired_at === null ? undefined : new Date(Number(row.last_fired_at));
    return { ...data, etag: row.etag, ...(lastFiredAt !== undefined ? { lastFiredAt } : {}) };
  }
}

interface ReminderRow {
  data: string;
  etag: string;
  last_fired_at: string | null;
}
