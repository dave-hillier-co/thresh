import { DEFAULT_SERVICE_ID } from "@thresh/core/default-service-id";
import type { RedisClient } from "@thresh/streams/redis-stream-queue";
import type { StreamCursorStore } from "@thresh/streams/stream-cursor-store";

// Monotonic compare-and-set: only write `cursor` when it advances the
// existing value, so a stale commit racing in from a de-owned pulling agent
// (ownership handoff) can never rewind a newer commit from the new owner.
const COMMIT_IF_GREATER = `
local current = redis.call('GET', KEYS[1])
if current == false or tonumber(ARGV[1]) > tonumber(current) then
  redis.call('SET', KEYS[1], ARGV[1])
end
return 1`;

/**
 * `StreamCursorStore` backed by a plain Redis key per `(provider, queueIdx)`,
 * following the same `<queueKey>:cursor` idiom `RedisStreamQueue` uses for
 * its own cursor — the Redis metadata option for a Kafka backing
 * (docs/stream-backings-postgres-kafka.md Phase 2), which has no cursor
 * surface of its own and needs to keep durable cursors somewhere other than
 * the transport. Partitioned by `serviceId` (issue #64, following #59's
 * `RedisGrainStorage` pattern); Redis has no ALTER, so a cursor written
 * before this dimension existed orphans on upgrade — a deliberate break (see
 * todo.md / docs/deviations.md).
 */
export class RedisStreamCursorStore implements StreamCursorStore {
  constructor(
    private readonly client: RedisClient,
    private readonly keyPrefix = "thresh",
    private readonly serviceId: string = DEFAULT_SERVICE_ID,
  ) {}

  private key(provider: string, queueIdx: number): string {
    return `${this.keyPrefix}:${this.serviceId}:streamq:${provider}:${queueIdx}:cursor`;
  }

  async getCursor(provider: string, queueIdx: number): Promise<number> {
    const value = await this.client.get(this.key(provider, queueIdx));
    return value === null ? 0 : Number(value);
  }

  async commit(provider: string, queueIdx: number, cursor: number): Promise<void> {
    await this.client.eval(COMMIT_IF_GREATER, {
      keys: [this.key(provider, queueIdx)],
      arguments: [String(cursor)],
    });
  }

  async seek(provider: string, queueIdx: number, cursor: number): Promise<void> {
    await this.client.set(this.key(provider, queueIdx), String(cursor));
  }
}
