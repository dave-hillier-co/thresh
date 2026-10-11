import type { Pool, PoolClient } from "pg";
import { raceSignal } from "@thresh/core/abort";
import { InconsistentStateError } from "@thresh/core/errors";
import type { GrainId } from "@thresh/core/grain-id";
import type { JournalEntry, JournalSegment, JournalStorage } from "@thresh/core/journal-storage";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";

export interface PostgresJournalStorageOptions {
  /**
   * Prefix for the two backing tables, `{prefix}_logs` and `{prefix}_entries`
   * (defaults to `"thresh_journal"`).
   */
  tablePrefix?: string;
  /**
   * Logical service identity this provider's logs belong to (Orleans'
   * `ClusterOptions.ServiceId`). Two clusters pointed at the same tables stay
   * partitioned by it. Defaults to `"default"`; `SiloBuilder` threads the
   * silo's `serviceId ?? DEFAULT_SERVICE_ID`.
   */
  serviceId?: string;
}

// Table names are interpolated (Postgres cannot bind identifiers), so guard
// them against anything but a plain SQL identifier; all other inputs are bound.
const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

// Concurrent silos can race `CREATE TABLE IF NOT EXISTS`; the loser sees a
// duplicate error even though the table now exists, which is the desired
// state. 42P07 duplicate_table, 23505 unique_violation (the catalogue's index).
function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07";
}

/**
 * Durable, cluster-shared journal store backed by Postgres. A log is one row
 * in `{prefix}_logs` (its CAS `version` and the next entry sequence number)
 * plus one row per entry in `{prefix}_entries`, ordered by `seq`. Appends and
 * snapshot replaces run in a transaction whose first statement is the
 * conditional version bump, so two silos racing on one log produce one winner
 * and one `InconsistentStateError` — the `GrainStorage` etag contract carried
 * over to a log, as `RedisJournalStorage` does with Lua. Appends write only
 * the new entries; nothing rewrites the log until a compaction replaces it.
 * `start()` creates the tables (idempotent); the host runs it on start.
 */
export class PostgresJournalStorage implements JournalStorage {
  private readonly logs: string;
  private readonly entries: string;
  private readonly serviceId: string;

  constructor(
    private readonly pool: Pool,
    options: PostgresJournalStorageOptions = {},
  ) {
    const prefix = options.tablePrefix ?? "thresh_journal";
    if (!IDENTIFIER.test(prefix)) throw new Error(`invalid table prefix: ${prefix}`);
    this.logs = `${prefix}_logs`;
    this.entries = `${prefix}_entries`;
    this.serviceId = options.serviceId ?? DEFAULT_SERVICE_ID;
  }

  async start(): Promise<void> {
    await this.createTable(
      `CREATE TABLE IF NOT EXISTS ${this.logs} (
         service_id text NOT NULL,
         grain_id text NOT NULL,
         log_name text NOT NULL,
         version integer NOT NULL,
         next_seq bigint NOT NULL,
         PRIMARY KEY (service_id, grain_id, log_name)
       )`,
    );
    await this.createTable(
      `CREATE TABLE IF NOT EXISTS ${this.entries} (
         service_id text NOT NULL,
         grain_id text NOT NULL,
         log_name text NOT NULL,
         seq bigint NOT NULL,
         entry text NOT NULL,
         PRIMARY KEY (service_id, grain_id, log_name, seq)
       )`,
    );
  }

  /**
   * `signal`, when given, only abandons the WAIT for an in-flight query
   * (`@thresh/core/abort`'s `raceSignal`) — node-postgres's `Pool.query` has no
   * abort hook in this major version, so the query itself keeps running
   * server-side (the same stance as `PostgresGrainStorage`).
   */
  async read(logName: string, grainId: GrainId, signal?: AbortSignal): Promise<JournalSegment> {
    return raceSignal(this.readSegment(logName, grainId), signal);
  }

  async append(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    signal?: AbortSignal,
  ): Promise<number> {
    return raceSignal(
      this.transact(async (client) => {
        const { version, firstSeq } = await this.bumpVersion(
          client,
          logName,
          grainId,
          entries.length,
          expectedVersion,
        );
        await this.insertEntries(client, logName, grainId, entries, firstSeq);
        return version;
      }),
      signal,
    );
  }

  async replace(
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    expectedVersion: number | undefined,
    signal?: AbortSignal,
  ): Promise<number> {
    return raceSignal(
      this.transact(async (client) => {
        const { version, firstSeq } = await this.bumpVersion(
          client,
          logName,
          grainId,
          entries.length,
          expectedVersion,
        );
        await client.query(
          `DELETE FROM ${this.entries}
           WHERE service_id = $1 AND grain_id = $2 AND log_name = $3 AND seq < $4`,
          [this.serviceId, grainId.toString(), logName, firstSeq],
        );
        await this.insertEntries(client, logName, grainId, entries, firstSeq);
        return version;
      }),
      signal,
    );
  }

  async clear(logName: string, grainId: GrainId, signal?: AbortSignal): Promise<void> {
    await raceSignal(
      this.transact(async (client) => {
        const key = [this.serviceId, grainId.toString(), logName];
        await client.query(
          `DELETE FROM ${this.entries} WHERE service_id = $1 AND grain_id = $2 AND log_name = $3`,
          key,
        );
        await client.query(
          `DELETE FROM ${this.logs} WHERE service_id = $1 AND grain_id = $2 AND log_name = $3`,
          key,
        );
      }),
      signal,
    );
  }

  private async readSegment(logName: string, grainId: GrainId): Promise<JournalSegment> {
    // One statement, so the version and the entries come from the same snapshot.
    const res = await this.pool.query<{ version: number; entry: string | null }>(
      `SELECT l.version, e.entry
       FROM ${this.logs} l
       LEFT JOIN ${this.entries} e
         ON e.service_id = l.service_id AND e.grain_id = l.grain_id AND e.log_name = l.log_name
       WHERE l.service_id = $1 AND l.grain_id = $2 AND l.log_name = $3
       ORDER BY e.seq`,
      [this.serviceId, grainId.toString(), logName],
    );
    const first = res.rows[0];
    if (first === undefined) return { entries: [], version: undefined };
    const entries = res.rows.flatMap((row) => (row.entry === null ? [] : [row.entry]));
    return { entries, version: first.version };
  }

  /**
   * The CAS step: advance the log's version iff it equals `expected`
   * (`undefined` = the log must not exist yet), reserving `count` sequence
   * numbers. Row-locks the log row for the rest of the transaction, so the
   * entry writes that follow cannot interleave with a racing writer's.
   */
  private async bumpVersion(
    client: PoolClient,
    logName: string,
    grainId: GrainId,
    count: number,
    expected: number | undefined,
  ): Promise<{ version: number; firstSeq: number }> {
    const key = [this.serviceId, grainId.toString(), logName];
    const res =
      expected === undefined
        ? await client.query<{ version: number; next_seq: string }>(
            `INSERT INTO ${this.logs} (service_id, grain_id, log_name, version, next_seq)
             VALUES ($1, $2, $3, 1, $4)
             ON CONFLICT DO NOTHING
             RETURNING version, next_seq`,
            [...key, count],
          )
        : await client.query<{ version: number; next_seq: string }>(
            `UPDATE ${this.logs}
             SET version = version + 1, next_seq = next_seq + $4
             WHERE service_id = $1 AND grain_id = $2 AND log_name = $3 AND version = $5
             RETURNING version, next_seq`,
            [...key, count, expected],
          );
    const row = res.rows[0];
    if (row === undefined) {
      const stored = await client.query<{ version: number }>(
        `SELECT version FROM ${this.logs} WHERE service_id = $1 AND grain_id = $2 AND log_name = $3`,
        key,
      );
      const current = stored.rows[0]?.version;
      throw new InconsistentStateError(
        `journal version conflict on ${logName} for ${grainId.toString()}`,
        expected === undefined ? undefined : String(expected),
        current === undefined ? undefined : String(current),
      );
    }
    return { version: row.version, firstSeq: Number(row.next_seq) - count };
  }

  private async insertEntries(
    client: PoolClient,
    logName: string,
    grainId: GrainId,
    entries: readonly JournalEntry[],
    firstSeq: number,
  ): Promise<void> {
    if (entries.length === 0) return;
    await client.query(
      `INSERT INTO ${this.entries} (service_id, grain_id, log_name, seq, entry)
       SELECT $1, $2, $3, $4 + ord - 1, entry
       FROM unnest($5::text[]) WITH ORDINALITY AS t(entry, ord)`,
      [this.serviceId, grainId.toString(), logName, firstSeq, entries],
    );
  }

  private async transact<T>(work: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await work(client);
      await client.query("COMMIT");
      return result;
    } catch (err) {
      await client.query("ROLLBACK").catch(() => {});
      throw err;
    } finally {
      client.release();
    }
  }

  private async createTable(ddl: string): Promise<void> {
    try {
      await this.pool.query(ddl);
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }
}
