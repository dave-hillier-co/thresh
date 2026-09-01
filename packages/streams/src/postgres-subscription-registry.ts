import type { Pool } from "pg";
import type { GrainId } from "@thresh/core/grain-id";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";
import type { SubscriptionRegistry } from "@thresh/streams/pulling-stream-provider-core";

const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710" || code === "42701";
}

/**
 * Durable pub-sub registry mapping a stream (`namespace/key`) to the set of
 * grains subscribed to it, backed by one row per `(service_id, provider,
 * stream_key, subscriber)` in `<prefix>_subscriptions` — the Postgres
 * counterpart to `RedisSubscriptionRegistry`'s Redis set, same semantics:
 * idempotent subscribe, `provider` in the primary key so several providers
 * can share a table prefix without colliding, `service_id` so several
 * services can too (issue #64, following #59's `PostgresGrainStorage`
 * pattern).
 */
export class PostgresSubscriptionRegistry implements SubscriptionRegistry {
  private readonly table: string;
  private readonly serviceId: string;

  constructor(
    private readonly pool: Pool,
    tablePrefix: string,
    private readonly providerName: string,
    serviceId: string = DEFAULT_SERVICE_ID,
  ) {
    this.table = `${tablePrefix}_subscriptions`;
    this.serviceId = serviceId;
    if (!IDENTIFIER.test(this.table)) throw new Error(`invalid table name: ${this.table}`);
  }

  /** Idempotently provisions the subscriptions table. */
  async start(): Promise<void> {
    try {
      await this.pool.query(
        `CREATE TABLE IF NOT EXISTS ${this.table} (
           provider TEXT NOT NULL,
           stream_key TEXT NOT NULL,
           subscriber TEXT NOT NULL,
           service_id TEXT NOT NULL,
           PRIMARY KEY (service_id, provider, stream_key, subscriber)
         )`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
    await this.addServiceIdColumn();
  }

  /**
   * Bring a table created before issue #64 up to the service-partitioned
   * shape, mirroring `PostgresGrainStorage.addServiceIdColumn()` (#59).
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
        `ALTER TABLE ${this.table} ADD PRIMARY KEY (service_id, provider, stream_key, subscriber)`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }

  async subscribe(streamKey: string, subscriber: GrainId): Promise<void> {
    await this.pool.query(
      `INSERT INTO ${this.table} (provider, stream_key, subscriber, service_id)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (service_id, provider, stream_key, subscriber) DO NOTHING`,
      [this.providerName, streamKey, serializeValue(subscriber), this.serviceId],
    );
  }

  async unsubscribe(streamKey: string, subscriber: GrainId): Promise<void> {
    await this.pool.query(
      `DELETE FROM ${this.table}
       WHERE provider = $1 AND stream_key = $2 AND subscriber = $3 AND service_id = $4`,
      [this.providerName, streamKey, serializeValue(subscriber), this.serviceId],
    );
  }

  async subscribers(streamKey: string): Promise<GrainId[]> {
    const res = await this.pool.query<{ subscriber: string }>(
      `SELECT subscriber FROM ${this.table}
       WHERE provider = $1 AND stream_key = $2 AND service_id = $3`,
      [this.providerName, streamKey, this.serviceId],
    );
    return res.rows.map((row) => deserializeValue<GrainId>(row.subscriber));
  }
}
