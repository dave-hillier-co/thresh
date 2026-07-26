import type { Pool } from "pg";
import type { GrainId } from "@thresh/core/grain-id";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import type { SubscriptionRegistry } from "@thresh/streams/pulling-stream-provider-core";

const IDENTIFIER = /^[a-z_][a-z0-9_]*$/;

function isDuplicate(err: unknown): boolean {
  const code = (err as { code?: string }).code;
  return code === "23505" || code === "42P07" || code === "42710";
}

/**
 * Durable pub-sub registry mapping a stream (`namespace/key`) to the set of
 * grains subscribed to it, backed by one row per `(provider, stream_key,
 * subscriber)` in `<prefix>_subscriptions` — the Postgres counterpart to
 * `RedisSubscriptionRegistry`'s Redis set, same semantics: idempotent
 * subscribe, `provider` in the primary key so several providers can share a
 * table prefix without colliding.
 */
export class PostgresSubscriptionRegistry implements SubscriptionRegistry {
  private readonly table: string;

  constructor(
    private readonly pool: Pool,
    tablePrefix: string,
    private readonly providerName: string,
  ) {
    this.table = `${tablePrefix}_subscriptions`;
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
           PRIMARY KEY (provider, stream_key, subscriber)
         )`,
      );
    } catch (err) {
      if (!isDuplicate(err)) throw err;
    }
  }

  async subscribe(streamKey: string, subscriber: GrainId): Promise<void> {
    await this.pool.query(
      `INSERT INTO ${this.table} (provider, stream_key, subscriber)
       VALUES ($1, $2, $3)
       ON CONFLICT DO NOTHING`,
      [this.providerName, streamKey, serializeValue(subscriber)],
    );
  }

  async unsubscribe(streamKey: string, subscriber: GrainId): Promise<void> {
    await this.pool.query(
      `DELETE FROM ${this.table} WHERE provider = $1 AND stream_key = $2 AND subscriber = $3`,
      [this.providerName, streamKey, serializeValue(subscriber)],
    );
  }

  async subscribers(streamKey: string): Promise<GrainId[]> {
    const res = await this.pool.query<{ subscriber: string }>(
      `SELECT subscriber FROM ${this.table} WHERE provider = $1 AND stream_key = $2`,
      [this.providerName, streamKey],
    );
    return res.rows.map((row) => deserializeValue<GrainId>(row.subscriber));
  }
}
