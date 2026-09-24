import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { afterAll, describe, expect, it } from "vitest";
import type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import { KafkaStreamQueue, KafkaTopicQueues } from "@thresh/streams/kafka-stream-queue";
import { MemoryStreamCursorStore } from "@thresh/streams/stream-cursor-store";
import type { Admin } from "kafkajs";
import type { QueueEntry } from "@thresh/streams/redis-stream-queue";

const KAFKA_BROKERS = process.env.KAFKA_BROKERS ?? "localhost:9092";

async function reachable(): Promise<Kafka | undefined> {
  // No `retry: { retries: 0 }` here: this same client is reused to build the
  // producers under test, and kafkajs's idempotent producer requires
  // retries to stay enabled.
  const kafka = new Kafka({
    clientId: "thresh-test-probe",
    brokers: [KAFKA_BROKERS],
    connectionTimeout: 2000,
    logLevel: 1, // ERROR only — kafkajs is chatty at the default level
  });
  const admin = kafka.admin();
  try {
    await admin.connect();
    await admin.disconnect();
    return kafka;
  } catch {
    return undefined;
  }
}

const kafka = await reachable();
const createdTopics: string[] = [];

async function createTopic(numPartitions: number): Promise<string> {
  const topic = `thresh_test_kq_${randomUUID().replace(/-/g, "")}`;
  const admin = kafka!.admin();
  await admin.connect();
  try {
    await admin.createTopics({
      waitForLeaders: true,
      topics: [{ topic, numPartitions }],
    });
  } finally {
    await admin.disconnect();
  }
  createdTopics.push(topic);
  return topic;
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 10_000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 25));
  }
}

/** Drains `count` entries from `queue`, advancing the read cursor like `QueuePullingAgent` does. */
async function drain(
  queue: KafkaStreamQueue,
  count: number,
  timeoutMs = 10_000,
  startCursor = 0,
): Promise<QueueEntry[]> {
  const collected: QueueEntry[] = [];
  let cursor = startCursor;
  const start = Date.now();
  while (collected.length < count) {
    if (Date.now() - start > timeoutMs) {
      throw new Error(`drain: timed out with ${collected.length}/${count}`);
    }
    const batch = await queue.readAfter(cursor, 128);
    if (batch.length === 0) {
      await new Promise((r) => setTimeout(r, 25));
      continue;
    }
    for (const entry of batch) {
      collected.push(entry);
      cursor = entry.token;
    }
  }
  return collected;
}

afterAll(async () => {
  if (kafka === undefined) return;
  if (createdTopics.length === 0) return;
  const admin = kafka.admin();
  await admin.connect();
  try {
    await admin.deleteTopics({ topics: createdTopics });
  } finally {
    await admin.disconnect();
  }
});

describe.skipIf(kafka === undefined)("KafkaTopicQueues / KafkaStreamQueue", () => {
  it("append returns a monotonic per-partition token derived from the record's offset", async () => {
    const topic = await createTopic(2);
    const cursors = new MemoryStreamCursorStore();
    const client = new KafkaTopicQueues(kafka!, topic, "prov-append", cursors);
    await client.start(2);
    try {
      await client.acquire(0);
      const t1 = await client.append(0, "s1", "a");
      const t2 = await client.append(0, "s1", "b");
      expect(t2).toBeGreaterThan(t1);
      expect(t2).toBe(t1 + 1);

      const queue = new KafkaStreamQueue(client, 0);
      const entries = await drain(queue, 2);
      expect(entries.map((e) => e.event)).toEqual(["a", "b"]);
      expect(entries.map((e) => e.token)).toEqual([t1, t2]);
      expect(entries.every((e) => e.streamKey === "s1")).toBe(true);
    } finally {
      await client.stop();
    }
  }, 20_000);

  it("commits advance the durable cursor store", async () => {
    const topic = await createTopic(1);
    const cursors = new MemoryStreamCursorStore();
    const client = new KafkaTopicQueues(kafka!, topic, "prov-commit", cursors);
    await client.start(1);
    try {
      await client.acquire(0);
      const queue = new KafkaStreamQueue(client, 0);
      await queue.append("s1", "x");
      const [entry] = await drain(queue, 1);
      await queue.commit(entry!.token);

      expect(await cursors.getCursor("prov-commit", 0)).toBe(entry!.token);
      expect(await queue.getCursor()).toBe(entry!.token);
    } finally {
      await client.stop();
    }
  }, 20_000);

  it("re-seeks to the committed cursor on re-acquire after simulated ownership loss", async () => {
    const topic = await createTopic(1);
    const cursors = new MemoryStreamCursorStore();
    const client = new KafkaTopicQueues(kafka!, topic, "prov-handoff", cursors);
    await client.start(1);
    try {
      await client.acquire(0);
      const queue = new KafkaStreamQueue(client, 0);
      await queue.append("s1", "m1");
      const [first] = await drain(queue, 1);
      await queue.commit(first!.token);

      // Ownership lost: release pauses/clears the in-memory buffer.
      client.release(0);

      // More events arrive while nobody owns the partition.
      await queue.append("s1", "m2");
      await queue.append("s1", "m3");

      // Ownership re-acquired: seeks to the durably committed cursor, not
      // wherever the paused consumer happened to be.
      await client.acquire(0);
      const rest = await drain(queue, 2);
      expect(rest.map((e) => e.event)).toEqual(["m2", "m3"]);
    } finally {
      await client.stop();
    }
  }, 20_000);

  it("pauses the partition once the buffer passes the high-water mark and resumes as it drains", async () => {
    const topic = await createTopic(1);
    const cursors = new MemoryStreamCursorStore();
    const highWaterMark = 5;
    const client = new KafkaTopicQueues(kafka!, topic, "prov-backpressure", cursors, {
      highWaterMark,
    });
    await client.start(1);
    try {
      await client.acquire(0);
      const queue = new KafkaStreamQueue(client, 0);
      for (let i = 0; i < 20; i++) await queue.append("s1", `e${i}`);

      // Give the consumer a moment to buffer well past the high-water mark
      // and pause the partition before draining starts.
      await new Promise((r) => setTimeout(r, 300));

      // Draining still delivers every event, in order, once resumed by readAfter.
      const entries = await drain(queue, 20, 30_000);
      expect(entries.map((e) => e.event)).toEqual(Array.from({ length: 20 }, (_, i) => `e${i}`));
    } finally {
      await client.stop();
    }
  }, 40_000);

  it("on a retention gap (committed cursor below the earliest offset) seeks to earliest and reports the gap", async () => {
    const topic = await createTopic(1);
    const cursors = new MemoryStreamCursorStore();
    const reports: Array<{ streamKey: string; token: number; error: unknown }> = [];
    const failureHandler: StreamFailureHandler = {
      onDeliveryFailure: (streamKey, _event, token, error) => {
        reports.push({ streamKey, token, error });
      },
    };
    const client = new KafkaTopicQueues(kafka!, topic, "prov-gap", cursors, { failureHandler });
    await client.start(1);
    try {
      await client.acquire(0);
      const queue = new KafkaStreamQueue(client, 0);
      await queue.append("s1", "m1");
      await queue.append("s1", "m2");
      await queue.append("s1", "m3");
      const [first] = await drain(queue, 1);
      await queue.commit(first!.token); // committed cursor sits at m1 (offset 0)
      client.release(0);

      // Force a real retention gap: delete every record up to (not
      // including) offset 3 — i.e. all of m1..m3 — so the partition's
      // earliest available offset moves past the committed cursor, exactly
      // what a long outage plus normal retention would do.
      const admin = kafka!.admin();
      await admin.connect();
      try {
        // Prime the admin client's metadata cache for this (freshly
        // created) topic — deleteTopicRecords needs to know the leader.
        await admin.fetchTopicMetadata({ topics: [topic] });
        await admin.deleteTopicRecords({ topic, partitions: [{ partition: 0, offset: "3" }] });
      } finally {
        await admin.disconnect();
      }

      await queue.append("s1", "m4"); // offset 3, the first record still on the partition

      await client.acquire(0);
      await waitFor(() => reports.length === 1);
      expect(reports[0]!.streamKey).toBe("prov-gap/queue-0");
      expect(reports[0]!.error).toBeInstanceOf(Error);

      // At-least-once is preserved for what still exists: m4 is delivered
      // once the seek lands on the new earliest offset.
      const [recovered] = await drain(queue, 1);
      expect(recovered!.event).toBe("m4");
    } finally {
      await client.stop();
    }
  }, 20_000);

  it("readAfter does not discard buffered entries the caller never confirmed (interrupted batch, issue #100)", async () => {
    const topic = await createTopic(1);
    const cursors = new MemoryStreamCursorStore();
    const client = new KafkaTopicQueues(kafka!, topic, "prov-partial-batch", cursors);
    await client.start(1);
    try {
      await client.acquire(0);
      const queue = new KafkaStreamQueue(client, 0);
      for (const event of ["m1", "m2", "m3", "m4", "m5"]) await queue.append("s1", event);

      // Read the whole batch, as `QueuePullingAgent.pump` does (`drain`
      // retries `readAfter` until all 5 have arrived — it never commits, so
      // this alone must not cost the queue anything).
      const all = await drain(queue, 5);
      expect(all.map((e) => e.event)).toEqual(["m1", "m2", "m3", "m4", "m5"]);

      // Only m1, m2 are ever durably confirmed — simulating a cursor-store
      // blip on the next commit that interrupts the rest of the batch.
      await queue.commit(all[1]!.token);

      // A read from the still-committed cursor must still see m3-m5: they
      // were handed back by the earlier full read but never confirmed, so
      // `readAfter` must not have thrown them away as a side effect of
      // returning them once already.
      const retry = await queue.readAfter(all[1]!.token, 5);
      expect(retry.map((e) => e.event)).toEqual(["m3", "m4", "m5"]);

      // And it must stay that way — repeating the read from the same
      // uncommitted cursor is idempotent, exactly like `RedisStreamQueue`'s
      // non-destructive `XRANGE`.
      const retryAgain = await queue.readAfter(all[1]!.token, 5);
      expect(retryAgain.map((e) => e.event)).toEqual(["m3", "m4", "m5"]);
    } finally {
      await client.stop();
    }
  }, 20_000);

  it("seek re-seeks the consumer so a rewind actually redelivers from the earlier cursor (issue #100)", async () => {
    const topic = await createTopic(1);
    const cursors = new MemoryStreamCursorStore();
    const client = new KafkaTopicQueues(kafka!, topic, "prov-rewind", cursors);
    await client.start(1);
    try {
      await client.acquire(0);
      const queue = new KafkaStreamQueue(client, 0);
      for (const event of ["m1", "m2", "m3", "m4", "m5"]) await queue.append("s1", event);

      const delivered = await drain(queue, 5);
      expect(delivered.map((e) => e.event)).toEqual(["m1", "m2", "m3", "m4", "m5"]);
      await queue.commit(delivered[4]!.token);

      // Simulate `RecoverableStreamDeliveryError(resume=<token after m2>)`:
      // the consumer wants to resume from its own checkpoint, rewinding past
      // what was already delivered.
      await queue.seek(delivered[1]!.token);

      // Rewind must actually re-seek the consumer / refill the buffer —
      // m3-m5 have to be redelivered, not lost.
      const redelivered = await drain(queue, 3, 15_000, delivered[1]!.token);
      expect(redelivered.map((e) => e.event)).toEqual(["m3", "m4", "m5"]);
    } finally {
      await client.stop();
    }
  }, 20_000);

  it("a failed acquire leaves the partition acquirable, so a retry really seeks and resumes (issue #113)", async () => {
    const topic = await createTopic(1);
    // Cursor store whose first read fails — the cursor-store blip that makes
    // `KafkaPartitionOwner` retry the acquire.
    let failNextRead = true;
    const inner = new MemoryStreamCursorStore();
    const cursors = {
      getCursor: async (provider: string, idx: number) => {
        if (failNextRead) {
          failNextRead = false;
          throw new Error("cursor store blip");
        }
        return inner.getCursor(provider, idx);
      },
      commit: (provider: string, idx: number, cursor: number) =>
        inner.commit(provider, idx, cursor),
      seek: (provider: string, idx: number, cursor: number) => inner.seek(provider, idx, cursor),
    };
    const client = new KafkaTopicQueues(kafka!, topic, "prov-acquire-retry", cursors);
    await client.start(1);
    try {
      await expect(client.acquire(0)).rejects.toThrow("cursor store blip");
      const queue = new KafkaStreamQueue(client, 0);
      await queue.append("s1", "m1");

      // The retry must not short-circuit on a half-finished earlier attempt:
      // it has to seek to the durable cursor and resume the paused partition.
      await client.acquire(0);
      const entries = await drain(queue, 1);
      expect(entries.map((e) => e.event)).toEqual(["m1"]);
    } finally {
      await client.stop();
    }
  }, 20_000);

  it("seek never lets records fetched from the old position into the rewound buffer (issue #100)", async () => {
    const topic = await createTopic(1);
    const cursors = new MemoryStreamCursorStore();
    // Hold the rewind's offset lookup open while one more record is
    // produced and fetched from the consumer's *old* (pre-rewind) position —
    // exactly what a live partition does during that admin round trip.
    let duringSeek: (() => Promise<void>) | undefined;
    const wrapAdmin = (admin: Admin): Admin =>
      new Proxy(admin, {
        get(target, prop, receiver) {
          if (prop === "fetchTopicOffsetsByTimestamp") {
            return async (...args: Parameters<Admin["fetchTopicOffsetsByTimestamp"]>) => {
              const hook = duringSeek;
              duringSeek = undefined;
              if (hook !== undefined) await hook();
              return target.fetchTopicOffsetsByTimestamp(...args);
            };
          }
          const value: unknown = Reflect.get(target, prop, receiver);
          return typeof value === "function" ? value.bind(target) : value;
        },
      });
    const wrapped = {
      producer: (...a: Parameters<Kafka["producer"]>) => kafka!.producer(...a),
      consumer: (...a: Parameters<Kafka["consumer"]>) => kafka!.consumer(...a),
      admin: (...a: Parameters<Kafka["admin"]>) => wrapAdmin(kafka!.admin(...a)),
    } as unknown as Kafka;
    const client = new KafkaTopicQueues(wrapped, topic, "prov-rewind-stale", cursors);
    await client.start(1);
    try {
      await client.acquire(0);
      const queue = new KafkaStreamQueue(client, 0);
      for (const event of ["m1", "m2", "m3", "m4", "m5"]) await queue.append("s1", event);
      const delivered = await drain(queue, 5);
      await queue.commit(delivered[4]!.token);

      duringSeek = async () => {
        await queue.append("s1", "m6");
        await waitFor(() => client.readAfter(0, 0, 1000).some((e) => e.event === "m6"));
      };
      await queue.seek(delivered[1]!.token);

      const redelivered = await drain(queue, 4, 15_000, delivered[1]!.token);
      expect(redelivered.map((e) => e.event)).toEqual(["m3", "m4", "m5", "m6"]);
    } finally {
      await client.stop();
    }
  }, 20_000);
});
