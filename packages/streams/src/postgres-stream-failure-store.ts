import type { Pool } from "pg";
import { deserializeValue, serializeValue } from "@tsva/core/value-codec";
import type {
  DurableStreamFailureStore,
  StreamDeliveryFailure,
} from "@tsva/streams/stream-failure-store";

const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710";
}

interface FailureRow {
  stream_key: string;
  payload: string;
  token: string;
  error: string;
  attempts: number;
  failed_at: string;
}

/**
 * Durable `DurableStreamFailureStore` backed by `<prefix>_failures` — one row
 * per permanently-failed (poison) delivery, the first durable implementation
 * (`MemoryStreamFailureStore` was the only one before this; Orleans'
 * `AzureTableStorageStreamFailureHandler` analog). Wrap in
 * `DurableStreamFailureHandler` to plug into a pulling provider's
 * `failureHandler` option, the same way any other `StreamFailureHandler` is.
 */
export class PostgresStreamFailureStore implements DurableStreamFailureStore {
  private readonly table: string;

  constructor(
    private readonly pool: Pool,
    tablePrefix = "tsva_stream",
  ) {
    this.table = `${tablePrefix}_failures`;
    if (!IDENTIFIER.test(this.table)) throw new Error(`invalid table name: ${this.table}`);
  }

  /** Idempotently provisions the failures table. */
  async start(): Promise<void> {
    try {
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS ${this.table} (
           id BIGSERIAL PRIMARY KEY,
           stream_key TEXT NOT NULL,
           payload TEXT NOT NULL,
           token BIGINT NOT NULL,
           error TEXT NOT NULL,
           attempts INT NOT NULL,
           failed_at BIGINT NOT NULL
         )`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }

  async recordFailure(failure: StreamDeliveryFailure): Promise<void> {
    await this.pool.query(
      `INSERT INTO ${this.table} (stream_key, payload, token, error, attempts, failed_at)
       VALUES ($1, $2, $3, $4, $5, $6)`,
      [
        failure.streamKey,
        serializeValue(failure.event),
        failure.token,
        failure.error,
        failure.attempts,
        failure.failedAt,
      ],
    );
  }

  async listFailures(streamKey?: string): Promise<StreamDeliveryFailure[]> {
    const res =
      streamKey === undefined
        ? await this.pool.query<FailureRow>(`SELECT * FROM ${this.table} ORDER BY id`)
        : await this.pool.query<FailureRow>(
            `SELECT * FROM ${this.table} WHERE stream_key = $1 ORDER BY id`,
            [streamKey],
          );
    return res.rows.map((row) => ({
      streamKey: row.stream_key,
      event: deserializeValue(row.payload),
      token: Number(row.token),
      error: row.error,
      attempts: row.attempts,
      failedAt: Number(row.failed_at),
    }));
  }
}
