import type { createClient } from "redis";
import { deserializeValue, serializeValue } from "@tsva/core/value-codec";

export type RedisClient = ReturnType<typeof createClient>;

/** One entry pulled from a physical queue: which stream it belongs to and its event. */
export interface QueueEntry {
  /** Monotonic, 1-based sequence within the queue; the consumer-facing token. */
  token: number;
  /** Canonical `namespace/key` of the stream this event was published to. */
  streamKey: string;
  event: unknown;
}

// Allocate a monotonic sequence and append in one atomic step so concurrent
// publishers never race the explicit stream id (XADD needs increasing ids). The
// entry records its stream key so the pulling agent can fan out to that stream's
// subscribers.
const APPEND = `
local n = redis.call('INCR', KEYS[2])
redis.call('XADD', KEYS[1], n .. '-0', 'stream', ARGV[1], 'data', ARGV[2])
return n`;

/**
 * One physical queue (a Redis Stream) onto which many logical streams are
 * multiplexed. Holds a durable, committed cursor so a pulling agent — or a
 * successor that takes the queue over on a membership change — resumes exactly
 * where the last one left off.
 */
export class RedisStreamQueue {
  private readonly seqKey: string;
  private readonly cursorKey: string;

  constructor(
    private readonly client: RedisClient,
    readonly key: string,
  ) {
    this.seqKey = `${key}:seq`;
    this.cursorKey = `${key}:cursor`;
  }

  /** Append an event for `streamKey`; returns its queue token. */
  async append(streamKey: string, event: unknown): Promise<number> {
    const token = await this.client.eval(APPEND, {
      keys: [this.key, this.seqKey],
      arguments: [streamKey, serializeValue(event)],
    });
    return token as number;
  }

  /** Entries with token strictly greater than `cursor`, in order. */
  async readAfter(cursor: number, count = 128): Promise<QueueEntry[]> {
    const entries = await this.client.xRange(this.key, `(${cursor}-0`, "+", { COUNT: count });
    return entries.map((entry) => ({
      token: Number.parseInt(entry.id, 10),
      streamKey: entry.message.stream ?? "",
      event: deserializeValue(entry.message.data ?? "null"),
    }));
  }

  /** The committed cursor (0 if the queue has never been read). */
  async getCursor(): Promise<number> {
    const value = await this.client.get(this.cursorKey);
    return value === null ? 0 : Number(value);
  }

  async commit(cursor: number): Promise<void> {
    await this.client.set(this.cursorKey, String(cursor));
  }
}
