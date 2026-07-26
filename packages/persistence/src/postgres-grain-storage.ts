import { randomUUID } from "node:crypto";
import type { Pool } from "pg";
import { raceSignal } from "@thresh/core/abort";
import { InconsistentStateError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import type { GrainStorage, StateHolder } from "@thresh/core/grain-storage";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";

export interface PostgresGrainStorageOptions {
  /** Table holding the state rows (defaults to `"thresh_grain_state"`). */
  tableName?: string;
}

// Table names are interpolated (Postgres cannot bind identifiers), so guard
// them against anything but a plain SQL identifier; all other inputs are bound.
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

// Concurrent silos can race `CREATE TABLE IF NOT EXISTS`; the loser sees a
// duplicate error even though the table now exists, which is the desired state.
function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07";
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

  constructor(
    private readonly pool: Pool,
    options: PostgresGrainStorageOptions = {},
  ) {
    this.table = options.tableName ?? "thresh_grain_state";
    if (!IDENTIFIER.test(this.table)) throw new Error(`invalid table name: ${this.table}`);
  }

  async start(): Promise<void> {
    try {
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS ${this.table} (
           grain_id text NOT NULL,
           state_name text NOT NULL,
           data text NOT NULL,
           etag text NOT NULL,
           PRIMARY KEY (grain_id, state_name)
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
        `SELECT data, etag FROM ${this.table} WHERE grain_id = $1 AND state_name = $2`,
        [grainId.toString(), stateName],
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
        `INSERT INTO ${this.table} (grain_id, state_name, data, etag)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (grain_id, state_name) DO UPDATE
         SET data = EXCLUDED.data, etag = EXCLUDED.etag
         WHERE ${this.table}.etag = $5
       RETURNING etag`,
        [grainId.toString(), stateName, serializeValue(state.value), etag, state.etag ?? ""],
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
         SELECT etag FROM ${this.table} WHERE grain_id = $1 AND state_name = $2
       ), deleted AS (
         DELETE FROM ${this.table} WHERE grain_id = $1 AND state_name = $2 AND etag = $3
         RETURNING 1
       )
       SELECT (SELECT etag FROM existing) AS stored_etag,
              EXISTS (SELECT 1 FROM existing) AS present,
              EXISTS (SELECT 1 FROM deleted) AS deleted`,
        [grainId.toString(), stateName, state.etag ?? ""],
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
      `SELECT etag FROM ${this.table} WHERE grain_id = $1 AND state_name = $2`,
      [grainId.toString(), stateName],
    );
    return new InconsistentStateError(
      `etag conflict ${verb} ${stateName} for ${grainId.toString()}`,
      expected,
      res.rows[0]?.etag,
    );
  }
}
