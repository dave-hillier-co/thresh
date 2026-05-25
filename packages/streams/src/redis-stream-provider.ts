import { randomUUID } from "node:crypto";
import type { createClient } from "redis";
import { keyToString, type GrainKey } from "@tsva/core/grain-key";
import {
  SequenceToken,
  type AsyncStream,
  type StreamHandler,
  type StreamId,
  type StreamProvider,
  type StreamSubscriptionHandle,
  type SubscribeOptions,
} from "@tsva/core/stream";
import { deserializeValue, serializeValue } from "@tsva/core/value-codec";

export type RedisClient = ReturnType<typeof createClient>;

export interface RedisStreamProviderOptions {
  keyPrefix?: string;
  /** How often each subscription polls Redis for new events (defaults to 50ms). */
  pollIntervalMs?: number;
}

// Allocate a monotonic sequence and append in one atomic step so concurrent
// publishers from different silos never race the explicit stream id (XADD
// requires strictly increasing ids). The sequence is the consumer-facing token.
const PUBLISH = `
local n = redis.call('INCR', KEYS[2])
redis.call('XADD', KEYS[1], n .. '-0', 'data', ARGV[1])
return n`;

/**
 * Durable, cluster-shared stream provider backed by Redis Streams. Events are
 * appended with XADD (ordered, persistent); each consumer's cursor is stored in
 * Redis so a reactivated consumer — even on a different silo after a restart —
 * resumes from exactly where it left off. Delivery is a non-blocking XRANGE poll
 * (so it shares the silo's Redis connection), and the cursor advances only after
 * `onNext` resolves, giving at-least-once delivery. Interchangeable with
 * `MemoryStreamProvider`.
 */
export class RedisStreamProvider implements StreamProvider {
  private readonly streams = new Map<string, RedisStream<unknown>>();
  private readonly keyPrefix: string;
  private readonly pollIntervalMs: number;

  constructor(
    private readonly client: RedisClient,
    private readonly name = "default",
    options: RedisStreamProviderOptions = {},
  ) {
    this.keyPrefix = options.keyPrefix ?? "tsva";
    this.pollIntervalMs = options.pollIntervalMs ?? 50;
  }

  getStream<T>(namespace: string, key: GrainKey): AsyncStream<T> {
    const keyString = keyToString(key);
    const mapKey = `${namespace}/${keyString}`;
    let stream = this.streams.get(mapKey);
    if (stream === undefined) {
      stream = new RedisStream<unknown>(
        this.client,
        { provider: this.name, namespace, key: keyString },
        `${this.keyPrefix}:stream:${this.name}:${mapKey}`,
        this.pollIntervalMs,
      );
      this.streams.set(mapKey, stream);
    }
    return stream as AsyncStream<T>;
  }

  /** Stop every poll loop (e.g. on silo shutdown). Durable cursors are kept. */
  async stop(): Promise<void> {
    for (const stream of this.streams.values()) stream.stopAll();
  }
}

class RedisStream<T> implements AsyncStream<T> {
  private readonly seqKey: string;
  private readonly subscriptions = new Map<string, RedisSubscription<T>>();

  constructor(
    private readonly client: RedisClient,
    readonly id: StreamId,
    private readonly streamKey: string,
    private readonly pollIntervalMs: number,
  ) {
    this.seqKey = `${streamKey}:seq`;
  }

  async publish(event: T): Promise<void> {
    await this.client.eval(PUBLISH, {
      keys: [this.streamKey, this.seqKey],
      arguments: [serializeValue(event)],
    });
  }

  async subscribe(
    handler: StreamHandler<T>,
    options?: SubscribeOptions,
  ): Promise<StreamSubscriptionHandle<T>> {
    const consumerId = options?.consumerId ?? `anon:${randomUUID()}`;
    // Default to "from now" (the current max sequence); a start token rewinds.
    const cursor = options?.startToken?.value ?? (await this.currentSeq());
    await this.client.set(this.cursorKey(consumerId), String(cursor));
    const sub = this.ensureSubscription(consumerId, cursor);
    sub.attach(handler);
    return sub;
  }

  async getSubscriptions(consumerId?: string): Promise<StreamSubscriptionHandle<T>[]> {
    if (consumerId === undefined) return [...this.subscriptions.values()];
    const existing = this.subscriptions.get(consumerId);
    if (existing !== undefined) return [existing];
    // A durable subscription left behind by an earlier activation/silo.
    const stored = await this.client.get(this.cursorKey(consumerId));
    if (stored === null) return [];
    return [this.ensureSubscription(consumerId, Number(stored))];
  }

  // --- internals shared with RedisSubscription ---

  private ensureSubscription(consumerId: string, cursor: number): RedisSubscription<T> {
    let sub = this.subscriptions.get(consumerId);
    if (sub === undefined) {
      sub = new RedisSubscription<T>(this, consumerId, cursor);
      this.subscriptions.set(consumerId, sub);
    }
    return sub;
  }

  async currentSeq(): Promise<number> {
    const value = await this.client.get(this.seqKey);
    return value === null ? 0 : Number(value);
  }

  /** Read events with sequence strictly greater than `cursor`, in order. */
  async readAfter(cursor: number): Promise<Array<{ seq: number; event: T }>> {
    const entries = await this.client.xRange(this.streamKey, `(${cursor}-0`, "+");
    return entries.map((entry) => ({
      seq: Number.parseInt(entry.id, 10),
      event: deserializeValue<T>(entry.message.data ?? "null"),
    }));
  }

  async persistCursor(consumerId: string, seq: number): Promise<void> {
    await this.client.set(this.cursorKey(consumerId), String(seq));
  }

  async deleteCursor(consumerId: string): Promise<void> {
    await this.client.del(this.cursorKey(consumerId));
  }

  remove(consumerId: string): void {
    this.subscriptions.delete(consumerId);
  }

  get intervalMs(): number {
    return this.pollIntervalMs;
  }

  stopAll(): void {
    for (const sub of this.subscriptions.values()) sub.pause();
  }

  private cursorKey(consumerId: string): string {
    return `${this.streamKey}:cursor:${consumerId}`;
  }
}

class RedisSubscription<T> implements StreamSubscriptionHandle<T> {
  private handler: StreamHandler<T> | undefined;
  private timer: ReturnType<typeof setTimeout> | undefined;
  private pumping = false;

  constructor(
    private readonly stream: RedisStream<T>,
    private readonly consumerId: string,
    private cursor: number,
  ) {}

  attach(handler: StreamHandler<T>): void {
    this.handler = handler;
    this.schedule(0);
  }

  async resume(handler: StreamHandler<T>): Promise<void> {
    this.handler = handler;
    this.schedule(0);
  }

  async unsubscribe(): Promise<void> {
    this.pause();
    this.stream.remove(this.consumerId);
    await this.stream.deleteCursor(this.consumerId);
  }

  /** Stop polling but keep the durable cursor (silo shutdown / deactivation). */
  pause(): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = undefined;
    this.handler = undefined;
  }

  private schedule(delayMs: number): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.timer = undefined;
      void this.pump();
    }, delayMs);
  }

  private async pump(): Promise<void> {
    if (this.pumping || this.handler === undefined) return;
    this.pumping = true;
    try {
      const events = await this.stream.readAfter(this.cursor);
      for (const { seq, event } of events) {
        if (this.handler === undefined) break;
        try {
          await this.handler.onNext(event, new SequenceToken(seq));
        } catch (err) {
          await this.handler.onError?.(err);
          break; // leave the cursor so the event is redelivered next poll
        }
        this.cursor = seq;
        await this.stream.persistCursor(this.consumerId, seq);
      }
    } finally {
      this.pumping = false;
      if (this.handler !== undefined) this.schedule(this.stream.intervalMs);
    }
  }
}
