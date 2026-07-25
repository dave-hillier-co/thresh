import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { afterAll, describe, expect, it } from "vitest";
import type { StreamFailureHandler } from "@tsva/streams/queue-pulling-agent";
import { KafkaStreamQueue, KafkaTopicQueues } from "@tsva/streams/kafka-stream-queue";
import { MemoryStreamCursorStore } from "@tsva/streams/stream-cursor-store";
import type { QueueEntry } from "@tsva/streams/redis-stream-queue";

const KAFKA_BROKERS = process.env.KAFKA_BROKERS ?? "localhost:9092";

async function reachable(): Promise<Kafka | undefined> {
  // No `retry: { retries: 0 }` here: this same client is reused to build the
  // producers under test, and kafkajs's idempotent producer requires
  // retries to stay enabled.
  const kafka = new Kafka({
    clientId: "tsva-test-probe",
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
  const topic = `tsva_test_kq_${randomUUID().replace(/-/g, "")}`;
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
): Promise<QueueEntry[]> {
  const collected: QueueEntry[] = [];
  let cursor = 0;
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
});
