import type { RedisClient } from "@thresh/streams/redis-stream-queue";
import type { StreamCursorStore } from "@thresh/streams/stream-cursor-store";

/**
 * `StreamCursorStore` backed by a plain Redis key per `(provider, queueIdx)`,
 * following the same `<queueKey>:cursor` idiom `RedisStreamQueue` uses for
 * its own cursor — the Redis metadata option for a Kafka backing
 * (docs/stream-backings-postgres-kafka.md Phase 2), which has no cursor
 * surface of its own and needs to keep durable cursors somewhere other than
 * the transport.
 */
export class RedisStreamCursorStore implements StreamCursorStore {
  constructor(
    private readonly client: RedisClient,
    private readonly keyPrefix = "thresh",
  ) {}

  private key(provider: string, queueIdx: number): string {
    return `${this.keyPrefix}:streamq:${provider}:${queueIdx}:cursor`;
  }

  async getCursor(provider: string, queueIdx: number): Promise<number> {
    const value = await this.client.get(this.key(provider, queueIdx));
    return value === null ? 0 : Number(value);
  }

  async commit(provider: string, queueIdx: number, cursor: number): Promise<void> {
    await this.client.set(this.key(provider, queueIdx), String(cursor));
  }
}
