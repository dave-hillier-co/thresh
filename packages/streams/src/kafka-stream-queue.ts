import { randomUUID } from "node:crypto";
import type { Admin, Consumer, Kafka, KafkaMessage, Producer } from "kafkajs";
import { deserializeValue, serializeValue } from "@thresh/core/value-codec";
import type { AppendableQueue } from "@thresh/streams/pulling-stream-provider-core";
import type { QueueEntry } from "@thresh/streams/redis-stream-queue";
import type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import type { StreamCursorStore } from "@thresh/streams/stream-cursor-store";

const DEFAULT_HIGH_WATER_MARK = 1000;
// Kafka's ListOffsets protocol timestamp constant for "earliest available offset".
const EARLIEST_TIMESTAMP = -2;

export interface KafkaTopicQueuesOptions {
  /** In-memory per-partition buffer size before the partition is paused (defaults to 1000). */
  highWaterMark?: number;
  /** Reported the retention-gap edge case (a seek target below the earliest offset). */
  failureHandler?: StreamFailureHandler;
}

/**
 * Drives one Kafka topic's partitions as a set of `AppendableQueue`s — one
 * per partition, which is also one physical queue
 * (docs/stream-backings-postgres-kafka.md Phase 2). A single producer and a
 * single consumer are shared across every partition of the topic; each
 * partition gets an in-memory buffer fed by the consumer's `eachMessage`,
 * paused/resumed at a high-water mark for backpressure — kafkajs's consumer
 * is push-styled, so this adapts it to `AppendableQueue`'s pull contract.
 *
 * kafkajs only exposes the high-level consumer-group API — there is no
 * `assign()` for manual partition ownership the way the low-level protocol
 * offers. Reusing consumer groups here would mean two systems fighting over
 * partition ownership: Kafka's group protocol and the port's
 * membership-driven hash ring (`refreshOwnership`), which reminders, durable
 * jobs and the Redis/Postgres stream backings all key off already. Instead
 * each `KafkaTopicQueues` uses a **unique, disposable groupId**: with exactly
 * one member the group protocol degenerates to "this consumer gets every
 * partition of the topic" (nothing to rebalance against), and
 * `seek`/`pause`/`resume` on top of that give the ring the same manual
 * control over which partitions are actively read that
 * `PullingStreamProviderCore.startAgentsFor` already exercises over which
 * queues run an agent. `KafkaPullingStreamProvider` calls `acquire`/`release`
 * to keep the two in lockstep: a partition is only resumed — and only after
 * being seeked to its durable cursor — while this silo's agent for it is
 * running; losing ownership pauses it again.
 */
export class KafkaTopicQueues {
  private readonly producer: Producer;
  private readonly consumer: Consumer;
  private readonly admin: Admin;
  private readonly buffers = new Map<number, QueueEntry[]>();
  private readonly pausedByBackpressure = new Set<number>();
  private readonly acquired = new Set<number>();
  private readonly highWaterMark: number;
  private readonly failureHandler: StreamFailureHandler | undefined;
  private started = false;

  constructor(
    kafka: Kafka,
    readonly topic: string,
    private readonly providerName: string,
    private readonly cursorStore: StreamCursorStore,
    options: KafkaTopicQueuesOptions = {},
  ) {
    this.highWaterMark = options.highWaterMark ?? DEFAULT_HIGH_WATER_MARK;
    this.failureHandler = options.failureHandler;
    this.producer = kafka.producer({ idempotent: true });
    this.consumer = kafka.consumer({
      groupId: `thresh-kafka-streams-${providerName}-${randomUUID()}`,
    });
    this.admin = kafka.admin();
  }

  /**
   * Connects the producer, consumer and admin client, validates the topic
   * exists with exactly `expectedPartitionCount` partitions (Kafka
   * auto-creation is assumed off — a missing topic or a partition-count
   * mismatch throws), and starts the consume loop. Thrown errors are plain
   * `Error`s; `KafkaPullingStreamProvider` wraps them as
   * `StreamProviderConfigurationError`.
   */
  async start(expectedPartitionCount: number): Promise<void> {
    if (this.started) return;
    await Promise.all([this.producer.connect(), this.consumer.connect(), this.admin.connect()]);
    let topics: { name: string; partitions: unknown[] }[];
    try {
      ({ topics } = await this.admin.fetchTopicMetadata({ topics: [this.topic] }));
    } catch (err) {
      await this.disconnectAll();
      throw new Error(
        `Kafka topic "${this.topic}" is not reachable/does not exist (topic auto-creation is ` +
          `disabled — create it explicitly with the desired partition count): ${String(err)}`,
        { cause: err },
      );
    }
    const meta = topics.find((t) => t.name === this.topic);
    if (meta === undefined) {
      await this.disconnectAll();
      throw new Error(
        `Kafka topic "${this.topic}" does not exist (topic auto-creation is disabled — create ` +
          `it explicitly with queueCount=${expectedPartitionCount} partitions)`,
      );
    }
    if (meta.partitions.length !== expectedPartitionCount) {
      await this.disconnectAll();
      throw new Error(
        `Kafka topic "${this.topic}" has ${meta.partitions.length} partitions, but queueCount ` +
          `is ${expectedPartitionCount} — they must match (partition = physical queue)`,
      );
    }

    this.started = true;
    await this.consumer.subscribe({ topic: this.topic, fromBeginning: false });
    // `pause` requires `run` to have been called first (kafkajs tracks
    // paused state on the running consumer group), so start the consume
    // loop and only then pause every partition — nothing is acquired yet,
    // so nothing should buffer before a queue is actually owned (`acquire`
    // resumes it). The brief window between `run` and `pause` can admit a
    // handful of messages into a not-yet-owned partition's buffer; that is
    // harmless (`acquire` resets the buffer before seeking/resuming).
    void this.consumer.run({
      eachMessage: async ({ partition, message }) => {
        this.onMessage(partition, message);
      },
    });
    this.consumer.pause([{ topic: this.topic }]);
  }

  async stop(): Promise<void> {
    if (!this.started) return;
    this.started = false;
    await this.disconnectAll();
  }

  private async disconnectAll(): Promise<void> {
    await Promise.all([
      this.consumer.disconnect(),
      this.producer.disconnect(),
      this.admin.disconnect(),
    ]);
  }

  private onMessage(partition: number, message: KafkaMessage): void {
    const buf = this.bufferFor(partition);
    buf.push({
      // Kafka offsets are 0-based, but every `PullableQueue`/cursor-store
      // convention in this codebase treats cursor 0 as the "nothing
      // committed yet" sentinel and expects tokens to start above it (the
      // Postgres BIGSERIAL and Redis INCR-allocated tokens both start at 1).
      // Offset-as-token would make the very first message (offset 0)
      // indistinguishable from "already delivered" for a fresh cursor, so
      // the token is offset + 1; `acquire`'s seek math undoes the shift.
      token: Number(message.offset) + 1,
      streamKey: message.key?.toString() ?? "",
      event: deserializeValue(message.value?.toString() ?? "null"),
    });
    if (buf.length > this.highWaterMark && !this.pausedByBackpressure.has(partition)) {
      this.pausedByBackpressure.add(partition);
      this.consumer.pause([{ topic: this.topic, partitions: [partition] }]);
    }
  }

  private bufferFor(partition: number): QueueEntry[] {
    let buf = this.buffers.get(partition);
    if (buf === undefined) {
      buf = [];
      this.buffers.set(partition, buf);
    }
    return buf;
  }

  /**
   * Called when this silo's ring ownership acquires partition `idx`'s queue:
   * seeks the consumer to the durably committed cursor + 1 and resumes it.
   * If the committed cursor has fallen below the partition's earliest
   * available offset (retention deleted it during an outage), seeks to
   * earliest instead and reports the gap through the failure handler —
   * at-least-once is preserved for what still exists, and the gap is
   * surfaced rather than silently skipped.
   */
  async acquire(idx: number): Promise<void> {
    if (this.acquired.has(idx)) return;
    this.acquired.add(idx);
    this.buffers.set(idx, []);
    const cursor = await this.cursorStore.getCursor(this.providerName, idx);
    const target = await this.resolveStartOffset(idx, cursor);
    this.consumer.seek({ topic: this.topic, partition: idx, offset: String(target) });
    this.pausedByBackpressure.delete(idx);
    this.consumer.resume([{ topic: this.topic, partitions: [idx] }]);
  }

  /** Called when this silo loses ownership of partition `idx`'s queue. */
  release(idx: number): void {
    if (!this.acquired.has(idx)) return;
    this.acquired.delete(idx);
    this.pausedByBackpressure.delete(idx);
    this.consumer.pause([{ topic: this.topic, partitions: [idx] }]);
    this.buffers.set(idx, []);
  }

  /**
   * Kafka offset (not token — see `onMessage`'s note on the +1 shift) to
   * seek to for `cursor`: the next undelivered token is `cursor + 1`, which
   * is offset `cursor + 1 - 1 = cursor`. So a never-committed cursor (0)
   * correctly seeks to offset 0, the topic's very first record.
   */
  private async resolveStartOffset(idx: number, cursor: number): Promise<number> {
    const desiredOffset = cursor;
    const earliestOffsets = await this.admin.fetchTopicOffsetsByTimestamp(
      this.topic,
      EARLIEST_TIMESTAMP,
    );
    const earliestEntry = earliestOffsets.find((o) => o.partition === idx);
    const earliest = earliestEntry === undefined ? 0 : Number(earliestEntry.offset);
    if (desiredOffset < earliest) {
      const gap = new Error(
        `retention gap on ${this.providerName}/queue-${idx}: committed cursor ${cursor} ` +
          `(offset ${desiredOffset}) is below the earliest available offset ${earliest}; ` +
          `seeking to earliest`,
      );
      console.error(gap.message);
      try {
        await this.failureHandler?.onDeliveryFailure(
          `${this.providerName}/queue-${idx}`,
          undefined,
          cursor,
          gap,
          0,
        );
      } catch {
        // Swallow handler errors — observability must not block recovery.
      }
      return earliest;
    }
    return desiredOffset;
  }

  /** Appends the event; returns its token (the record's offset + 1 — see `onMessage`). */
  async append(idx: number, streamKey: string, event: unknown): Promise<number> {
    const results = await this.producer.send({
      topic: this.topic,
      acks: -1,
      messages: [{ partition: idx, key: streamKey, value: serializeValue(event) }],
    });
    const result = results[0]!;
    if (result.baseOffset === undefined) {
      throw new Error(`kafka producer did not return an offset for ${this.topic}/${idx}`);
    }
    return Number(result.baseOffset) + 1;
  }

  /** Entries with token strictly greater than `cursor`, in publish order. */
  readAfter(idx: number, cursor: number, count = 128): QueueEntry[] {
    const buf = this.bufferFor(idx);
    while (buf.length > 0 && buf[0]!.token <= cursor) buf.shift();
    const batch = buf.splice(0, count);
    if (this.pausedByBackpressure.has(idx) && buf.length <= this.highWaterMark / 2) {
      this.pausedByBackpressure.delete(idx);
      this.consumer.resume([{ topic: this.topic, partitions: [idx] }]);
    }
    return batch;
  }

  getCursor(idx: number, signal?: AbortSignal): Promise<number> {
    return this.cursorStore.getCursor(this.providerName, idx, signal);
  }

  commit(idx: number, cursor: number, signal?: AbortSignal): Promise<void> {
    return this.cursorStore.commit(this.providerName, idx, cursor, signal);
  }
}

/** One partition of a `KafkaTopicQueues` topic, viewed as an `AppendableQueue`. */
export class KafkaStreamQueue implements AppendableQueue {
  constructor(
    private readonly client: KafkaTopicQueues,
    private readonly idx: number,
  ) {}

  async append(streamKey: string, event: unknown): Promise<number> {
    return this.client.append(this.idx, streamKey, event);
  }

  async readAfter(cursor: number, count = 128): Promise<QueueEntry[]> {
    return this.client.readAfter(this.idx, cursor, count);
  }

  async getCursor(): Promise<number> {
    return this.client.getCursor(this.idx);
  }

  async commit(cursor: number): Promise<void> {
    await this.client.commit(this.idx, cursor);
  }
}
