import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { Pool } from "pg";
import { afterAll, describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import { KafkaPullingStreamProvider } from "@thresh/streams/kafka-pulling-stream-provider";
import { PostgresSubscriptionRegistry } from "@thresh/streams/postgres-subscription-registry";
import { PostgresStreamCursorStore } from "@thresh/streams/postgres-stream-cursor-store";
import { PostgresStreamFailureStore } from "@thresh/streams/postgres-stream-failure-store";
import {
  DurableStreamFailureHandler,
  type StreamDeliveryFailure,
} from "@thresh/streams/stream-failure-store";
import { StreamProviderConfigurationError } from "@thresh/streams/stream-provider-config-error";

const KAFKA_BROKERS = process.env.KAFKA_BROKERS ?? "localhost:9092";
const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

async function reachableKafka(): Promise<Kafka | undefined> {
  const kafka = new Kafka({
    clientId: "thresh-test-kpsp",
    brokers: [KAFKA_BROKERS],
    connectionTimeout: 2000,
    logLevel: 1,
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

async function reachablePostgres(connectionString: string): Promise<Pool | undefined> {
  const probe = new Pool({ connectionString });
  probe.on("error", () => {});
  try {
    await probe.query("SELECT 1");
    return probe;
  } catch {
    await probe.end().catch(() => undefined);
    return undefined;
  }
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

const [kafka, pool] = await Promise.all([reachableKafka(), reachablePostgres(PG_URL)]);
const ready = kafka !== undefined && pool !== undefined;
const prefix = `thresh_test_kpsp_${randomUUID().replace(/-/g, "")}`;
const createdTopics: string[] = [];

/**
 * Creates a uniquely-prefixed topic named exactly the way
 * `KafkaPullingStreamProvider` derives it (`<topicPrefix>.<name>`) and
 * returns the `topicPrefix` to pass to the provider's constructor.
 */
async function createTopic(name: string, numPartitions: number): Promise<string> {
  const topicPrefix = `thresh_test_kpsp_${randomUUID().replace(/-/g, "")}`;
  const topic = `${topicPrefix}.${name}`;
  const admin = kafka!.admin();
  await admin.connect();
  try {
    await admin.createTopics({ waitForLeaders: true, topics: [{ topic, numPartitions }] });
  } finally {
    await admin.disconnect();
  }
  createdTopics.push(topic);
  return topicPrefix;
}

afterAll(async () => {
  if (kafka !== undefined && createdTopics.length > 0) {
    const admin = kafka.admin();
    await admin.connect();
    try {
      await admin.deleteTopics({ topics: createdTopics });
    } finally {
      await admin.disconnect();
    }
  }
  if (pool !== undefined) {
    for (const suffix of ["cursors", "subscriptions", "failures"]) {
      await pool.query(`DROP TABLE IF EXISTS ${prefix}_${suffix}`);
    }
    await pool.end();
  }
});

describe.skipIf(!ready)("KafkaPullingStreamProvider", () => {
  it("delivers a published event to an explicit subscriber", async () => {
    const name = "explicit";
    const topicPrefix = await createTopic(name, 1);
    const registry = new PostgresSubscriptionRegistry(pool!, prefix, name);
    const cursorStore = new PostgresStreamCursorStore(pool!, prefix);
    const provider = new KafkaPullingStreamProvider(kafka!, name, {
      topicPrefix,
      queueCount: 1,
      pollIntervalMs: 5,
      registry,
      cursorStore,
    });
    await provider.start();
    const received: string[] = [];
    provider.setDeliver(async (_subscriber, _streamKey, event) => {
      received.push(event as string);
    });
    provider.setImplicitSubscribers(() => []);

    try {
      provider.startAgentsFor([0]);
      await registry.subscribe("chat/room", new GrainId("ChatUser", "alice"));

      const stream = provider.getStream<string>("chat", "room");
      await stream.publish("hello");

      await waitFor(() => received.length === 1);
      expect(received).toEqual(["hello"]);
    } finally {
      provider.stop();
      await provider.disconnect();
    }
  }, 20_000);

  it("delivers to implicit subscribers with no explicit subscription", async () => {
    const name = "implicit";
    const topicPrefix = await createTopic(name, 1);
    const registry = new PostgresSubscriptionRegistry(pool!, prefix, name);
    const cursorStore = new PostgresStreamCursorStore(pool!, prefix);
    const provider = new KafkaPullingStreamProvider(kafka!, name, {
      topicPrefix,
      queueCount: 1,
      pollIntervalMs: 5,
      registry,
      cursorStore,
    });
    await provider.start();
    const delivered: Array<{ subscriber: string; event: unknown }> = [];
    provider.setDeliver(async (subscriber, _streamKey, event) => {
      delivered.push({ subscriber: subscriber.toString(), event });
    });
    provider.setImplicitSubscribers((namespace) => (namespace === "chat" ? ["ChatBot"] : []));

    try {
      provider.startAgentsFor([0]);
      const stream = provider.getStream<string>("chat", "lobby");
      await stream.publish("hi");

      await waitFor(() => delivered.length === 1);
      expect(delivered[0]!.event).toBe("hi");
      expect(delivered[0]!.subscriber).toBe("ChatBot/lobby");
    } finally {
      provider.stop();
      await provider.disconnect();
    }
  }, 20_000);

  it("resumes delivery from the committed cursor for a fresh provider instance (handoff)", async () => {
    const name = "handoff";
    const topicPrefix = await createTopic(name, 1);
    const registry = new PostgresSubscriptionRegistry(pool!, prefix, name);
    const cursorStore = new PostgresStreamCursorStore(pool!, prefix);
    const opts = {
      topicPrefix,
      queueCount: 1,
      pollIntervalMs: 5,
      registry,
      cursorStore,
    };
    const first = new KafkaPullingStreamProvider(kafka!, name, opts);
    await first.start();
    const received: string[] = [];
    first.setDeliver(async (_s, _k, event) => void received.push(event as string));
    first.setImplicitSubscribers((ns) => (ns === "chat" ? ["Listener"] : []));

    first.startAgentsFor([0]);
    await first.getStream<string>("chat", "room").publish("m1");
    await waitFor(() => received.length === 1);
    first.stop();
    await first.disconnect();

    // A fresh provider instance (new agents, new Kafka consumer) picks up
    // from the durably committed cursor: publishing more events before it
    // starts must not be lost, and the already-delivered event must not be
    // redelivered.
    const publisher = new KafkaPullingStreamProvider(kafka!, name, opts);
    await publisher.start();
    await publisher.getStream<string>("chat", "room").publish("m2");
    await publisher.disconnect();

    const second = new KafkaPullingStreamProvider(kafka!, name, opts);
    await second.start();
    second.setDeliver(async (_s, _k, event) => void received.push(event as string));
    second.setImplicitSubscribers((ns) => (ns === "chat" ? ["Listener"] : []));
    try {
      second.startAgentsFor([0]);
      await waitFor(() => received.length === 2);
      expect(received).toEqual(["m1", "m2"]);
    } finally {
      second.stop();
      await second.disconnect();
    }
  }, 30_000);

  it("skips a poison event after exhausting retries and records it to the durable failure store", async () => {
    const name = "poison";
    const topicPrefix = await createTopic(name, 1);
    const registry = new PostgresSubscriptionRegistry(pool!, prefix, name);
    const cursorStore = new PostgresStreamCursorStore(pool!, prefix);
    const failureStore = new PostgresStreamFailureStore(pool!, prefix);
    await failureStore.start();
    const failureHandler: StreamFailureHandler = new DurableStreamFailureHandler(failureStore);

    const provider = new KafkaPullingStreamProvider(kafka!, name, {
      topicPrefix,
      queueCount: 1,
      pollIntervalMs: 5,
      registry,
      cursorStore,
      failureHandler,
    });
    await provider.start();
    provider.setDeliver(async () => {
      throw new Error("always fails");
    });
    provider.setImplicitSubscribers((ns) => (ns === "room" ? ["poison-grain"] : []));

    try {
      provider.startAgentsFor([0]);
      await provider.getStream<string>("room", "bad").publish("poison");

      await waitFor(async () => {
        const failures: StreamDeliveryFailure[] = await failureStore.listFailures();
        return failures.length === 1;
      });
      const failures = await failureStore.listFailures();
      expect(failures).toHaveLength(1);
      expect(failures[0]!.streamKey).toBe("room/bad");
      expect(failures[0]!.attempts).toBe(3);
    } finally {
      provider.stop();
      await provider.disconnect();
    }
  }, 20_000);

  it("registers and unregisters an explicit producer", async () => {
    const name = "producers";
    const topicPrefix = await createTopic(name, 1);
    const registry = new PostgresSubscriptionRegistry(pool!, prefix, name);
    const cursorStore = new PostgresStreamCursorStore(pool!, prefix);
    const provider = new KafkaPullingStreamProvider(kafka!, name, {
      topicPrefix,
      queueCount: 1,
      registry,
      cursorStore,
    });
    await provider.start();
    try {
      const handle = await provider.registerProducer("chat", "room-1");
      expect(handle.streamId).toEqual({ provider: name, namespace: "chat", key: "room-1" });
      await handle.unregister();
      // Idempotent.
      await handle.unregister();
    } finally {
      await provider.disconnect();
    }
  }, 20_000);

  it("throws StreamProviderConfigurationError when queueCount doesn't match the topic's partition count", async () => {
    const name = "mismatch";
    const topicPrefix = await createTopic(name, 3);
    const registry = new PostgresSubscriptionRegistry(pool!, prefix, name);
    const cursorStore = new PostgresStreamCursorStore(pool!, prefix);
    const provider = new KafkaPullingStreamProvider(kafka!, name, {
      topicPrefix,
      queueCount: 2, // topic actually has 3 partitions
      registry,
      cursorStore,
    });

    await expect(provider.start()).rejects.toThrow(StreamProviderConfigurationError);
  }, 20_000);

  it("throws StreamProviderConfigurationError when the topic does not exist", async () => {
    const name = `missing-${randomUUID().replace(/-/g, "")}`;
    const registry = new PostgresSubscriptionRegistry(pool!, prefix, name);
    const cursorStore = new PostgresStreamCursorStore(pool!, prefix);
    const provider = new KafkaPullingStreamProvider(kafka!, name, {
      topicPrefix: "thresh.streams.nonexistent",
      queueCount: 1,
      registry,
      cursorStore,
    });

    await expect(provider.start()).rejects.toThrow(StreamProviderConfigurationError);
  }, 20_000);
});
