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

// Concurrent silos can race `CREATE TABLE IF NOT EXISTS`; the loser sees a duplicate error even though the object now exists,
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
    const expected = state.etag ?? "";
    const key = [this.serviceId, grainId.toString(), stateName];
    const data = serializeValue(state.value);
    // Two single-statement paths, chosen by whether the caller holds an etag,
    // mirroring Orleans' AdoNet `CheckVersionInconsistency` (issue #109):
    //  - blind write (`''`, never a UUID): insert only if no row exists; a row
    //    that does exist is a conflict (zero rows returned).
    //  - versioned write: update only the row whose etag still matches. A
    //    stale etag, or a row that has gone missing, updates zero rows — so a
    //    stale writer can never resurrect a cleared record. The UPDATE takes
    //    the row lock, so a concurrent in-flight DELETE is waited out and then
    //    re-checked rather than raced (an INSERT ... WHERE EXISTS guard reads
    //    a snapshot that can still see the row being deleted, and inserts).
    const res = await raceSignal(
      expected === ""
        ? this.pool.query<{ etag: string }>(
            `INSERT INTO ${this.table} (service_id, grain_id, state_name, data, etag)
             VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (service_id, grain_id, state_name) DO NOTHING
             RETURNING etag`,
            [...key, data, etag],
          )
        : this.pool.query<{ etag: string }>(
            `UPDATE ${this.table} SET data = $4, etag = $5
             WHERE service_id = $1 AND grain_id = $2 AND state_name = $3 AND etag = $6
             RETURNING etag`,
            [...key, data, etag, expected],
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
    // A present row that did not delete (blank or stale etag) is a conflict,
    // as before; a MISSING row with a non-empty expected etag is now one too
    // (issue #109) — otherwise a stale clearer would treat someone else's
    // earlier delete as a no-op success rather than the conflict it is.
    const expected = state.etag ?? "";
    if ((row.present && !row.deleted) || (!row.present && expected !== "")) {
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
