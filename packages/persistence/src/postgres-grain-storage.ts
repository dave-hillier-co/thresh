import { randomUUID } from "node:crypto";
import type { Pool } from "pg";
import { raceSignal } from "@thresh/core/abort";
import { InconsistentStateError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainStorage, StateHolder } from "@thresh/core/grain-storage";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";

export interface PostgresGrainStorageOptions {
  /** Table holding the state rows (defaults to `"thresh_grain_state"`). */
  tableName?: string;
  /**
   * Logical service identity this provider's rows belong to (Orleans'
   * `ClusterOptions.ServiceId`, which its AdoNet provider carries as
   * `OrleansStorage.serviceid` and includes in every row key). Two clusters
   * pointed at the same table stay partitioned by it. Defaults to `"default"`,
   * Orleans' own `ClusterOptions.DefaultServiceId`; `SiloBuilder` threads the
   * silo's `serviceId ?? DEFAULT_SERVICE_ID`.
   */
  serviceId?: string;
}

// Table names are interpolated (Postgres cannot bind identifiers), so guard
// them against anything but a plain SQL identifier; all other inputs are bound.
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

// Concurrent silos can race `CREATE TABLE IF NOT EXISTS` and the migration
// below; the loser sees a duplicate error even though the object now exists,
// which is the desired state. 42P07 duplicate_table, 42710 duplicate_object
// (a constraint), 42701 duplicate_column, 23505 unique_violation (the
// catalogue's own index).
function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710" || code === "42701";
}

/**
 * Durable, cluster-shared storage provider backed by Postgres. Each named state
 * is one row (`grain_id`, `state_name`, serialized `data`, `etag`); writes and
 * clears are conditional single statements so optimistic concurrency holds
 * across silos — the same etag contract as `MemoryGrainStorage` /
 * `RedisGrainStorage`. Good when state must also be queried outside the actor
 * model. `start()` creates the table (idempotent); the host runs it on start.
 */
export class PostgresGrainStorage implements GrainStorage {
  private readonly table: string;
  private readonly serviceId: string;

  constructor(
    private readonly pool: Pool,
    options: PostgresGrainStorageOptions = {},
  ) {
    this.table = options.tableName ?? "thresh_grain_state";
    this.serviceId = options.serviceId ?? DEFAULT_SERVICE_ID;
    if (!IDENTIFIER.test(this.table)) throw new Error(`invalid table name: ${this.table}`);
  }

  async start(): Promise<void> {
    try {
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS ${this.table} (
           grain_id text NOT NULL,
           state_name text NOT NULL,
           service_id text NOT NULL,
           data text NOT NULL,
           etag text NOT NULL,
           PRIMARY KEY (service_id, grain_id, state_name)
         )`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
    await this.addServiceIdColumn();
  }

  /**
   * Bring a table created before issue #59 up to the service-partitioned shape.
   * `CREATE TABLE IF NOT EXISTS` above does nothing to an EXISTING table, so a
   * pre-#59 table would silently keep `PRIMARY KEY (grain_id, state_name)` and
   * every statement here would fail (`ON CONFLICT (service_id, ...)` with 42P10,
   * the rest with "column service_id does not exist"). Existing rows backfill to
   * `DEFAULT_SERVICE_ID`, which is also what a provider configured with no
   * `serviceId` reads, so a single-cluster deployment sees no change.
   *
   * Idempotent, and safe for two silos racing it: each statement either no-ops
   * or raises a duplicate error, which is the state we wanted.
   */
  private async addServiceIdColumn(): Promise<void> {
    const existing = await this.pool.query(
      `SELECT 1 FROM information_schema.columns
       WHERE table_schema = current_schema() AND table_name = $1 AND column_name = 'service_id'`,
      [this.table],
    );
    if (existing.rowCount !== 0) return;
    try {
      // The default backfills existing rows, then goes away: a fresh table has
      // no default on the column, and neither should a migrated one.
      await this.pool.query(
        `ALTER TABLE ${this.table}
           ADD COLUMN IF NOT EXISTS service_id text NOT NULL DEFAULT '${DEFAULT_SERVICE_ID}'`,
      );
      await this.pool.query(`ALTER TABLE ${this.table} ALTER COLUMN service_id DROP DEFAULT`);
      // The old key was `(grain_id, state_name)`. Look its constraint name up
      // rather than assuming Postgres' implicit `<table>_pkey`, which it
      // truncates past 63 characters — a guess that missed would leave the old
      // key in place and fail the ADD below.
      const pk = await this.pool.query<{ conname: string }>(
        `SELECT conname FROM pg_constraint WHERE contype = 'p' AND conrelid = to_regclass($1)`,
        [this.table],
      );
      const oldKey = pk.rows[0]?.conname;
      if (oldKey !== undefined) {
        await this.pool.query(`ALTER TABLE ${this.table} DROP CONSTRAINT "${oldKey}"`);
      }
      await this.pool.query(
        `ALTER TABLE ${this.table} ADD PRIMARY KEY (service_id, grain_id, state_name)`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }

  /**
   * `signal`, when given, only abandons the WAIT for an in-flight query
   * (`@thresh/core/abort`'s `raceSignal`) — node-postgres's `Pool.query` has no
   * abort hook in this major version, so the query itself keeps running
   * server-side; unlike `RedisGrainStorage`, which cancels for real via
   * node-redis's `withAbortSignal`.
   */
  async read<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    const res = await raceSignal(
      this.pool.query<{ data: string; etag: string }>(
        `SELECT data, etag FROM ${this.table}
         WHERE service_id = $1 AND grain_id = $2 AND state_name = $3`,
        [this.serviceId, grainId.toString(), stateName],
      ),
      signal,
    );
    const row = res.rows[0];
    if (row === undefined) {
      state.exists = false;
      state.etag = undefined;
      return;
    }
    state.value = deserializeValue<T>(row.data);
    state.etag = row.etag;
    state.exists = true;
  }

  async write<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    const etag = randomUUID();
    // No row -> insert succeeds regardless of expected etag (matches the memory
    // provider). An existing row only updates when the caller's etag still
    // matches; a blind (`''`, never a UUID) or stale etag updates zero rows.
    const res = await raceSignal(
      this.pool.query<{ etag: string }>(
        `INSERT INTO ${this.table} (service_id, grain_id, state_name, data, etag)
       VALUES ($1, $2, $3, $4, $5)
       ON CONFLICT (service_id, grain_id, state_name) DO UPDATE
         SET data = EXCLUDED.data, etag = EXCLUDED.etag
         WHERE ${this.table}.etag = $6
       RETURNING etag`,
        [
          this.serviceId,
          grainId.toString(),
          stateName,
          serializeValue(state.value),
          etag,
          state.etag ?? "",
        ],
      ),
      signal,
    );
    if (res.rows.length === 0) {
      throw await this.conflict("writing", stateName, grainId, state.etag);
    }
    state.etag = etag;
    state.exists = true;
  }

  async clear<T>(
    stateName: string,
    grainId: GrainId,
    state: StateHolder<T>,
    signal?: AbortSignal,
  ): Promise<void> {
    // One atomic statement reports whether a row existed and whether the
    // etag-guarded delete fired: an absent row is a no-op; a present row that
    // did not delete (blank or stale etag) is a conflict.
    const res = await raceSignal(
      this.pool.query<{
        stored_etag: string | null;
        present: boolean;
        deleted: boolean;
      }>(
        `WITH existing AS (
         SELECT etag FROM ${this.table}
         WHERE service_id = $1 AND grain_id = $2 AND state_name = $3
       ), deleted AS (
         DELETE FROM ${this.table}
         WHERE service_id = $1 AND grain_id = $2 AND state_name = $3 AND etag = $4
         RETURNING 1
       )
       SELECT (SELECT etag FROM existing) AS stored_etag,
              EXISTS (SELECT 1 FROM existing) AS present,
              EXISTS (SELECT 1 FROM deleted) AS deleted`,
        [this.serviceId, grainId.toString(), stateName, state.etag ?? ""],
      ),
      signal,
    );
    const row = res.rows[0]!;
    if (row.present && !row.deleted) {
      throw new InconsistentStateError(
        `etag conflict clearing ${stateName} for ${grainId.toString()}`,
        state.etag,
        row.stored_etag ?? undefined,
      );
    }
    state.etag = undefined;
    state.exists = false;
  }

  private async conflict(
    verb: string,
    stateName: string,
    grainId: GrainId,
    expected: string | undefined,
  ): Promise<InconsistentStateError> {
    const res = await this.pool.query<{ etag: string }>(
      `SELECT etag FROM ${this.table}
       WHERE service_id = $1 AND grain_id = $2 AND state_name = $3`,
      [this.serviceId, grainId.toString(), stateName],
    );
    return new InconsistentStateError(
      `etag conflict ${verb} ${stateName} for ${grainId.toString()}`,
      expected,
      res.rows[0]?.etag,
    );
  }
}
