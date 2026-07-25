import { randomUUID } from "node:crypto";
import { afterAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import { GrainId } from "@tsva/core/grain-id";
import type { StreamFailureHandler } from "@tsva/streams/queue-pulling-agent";
import { PostgresPullingStreamProvider } from "@tsva/streams/postgres-pulling-stream-provider";
import {
  DurableStreamFailureHandler,
  type StreamDeliveryFailure,
} from "@tsva/streams/stream-failure-store";
import { PostgresStreamFailureStore } from "@tsva/streams/postgres-stream-failure-store";
import { PostgresSubscriptionRegistry } from "@tsva/streams/postgres-subscription-registry";

const PG_URL = process.env.PG_URL ?? "postgres://localhost:5432/postgres";

async function reachable(connectionString: string): Promise<Pool | undefined> {
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
  timeoutMs = 4000,
): Promise<void> {
  const start = Date.now();
  while (!(await predicate())) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 10));
  }
}

const pool = await reachable(PG_URL);
const prefix = `tsva_test_pp_${randomUUID().replace(/-/g, "")}`;

afterAll(async () => {
  if (pool === undefined) return;
  for (const suffix of ["events", "cursors", "subscriptions", "failures"]) {
    await pool.query(`DROP TABLE IF EXISTS ${prefix}_${suffix}`);
  }
  await pool.end();
});

describe.skipIf(pool === undefined)("PostgresPullingStreamProvider", () => {
  it("delivers a published event to an explicit subscriber", async () => {
    const provider = new PostgresPullingStreamProvider(pool!, "explicit", {
      tablePrefix: prefix,
      queueCount: 1,
      pollIntervalMs: 5,
    });
    await provider.start();
    const received: string[] = [];
    provider.setDeliver(async (_subscriber, _streamKey, event) => {
      received.push(event as string);
    });
    provider.setImplicitSubscribers(() => []);

    try {
      provider.startAgentsFor([0]);
      const registry = new PostgresSubscriptionRegistry(pool!, prefix, "explicit");
      await registry.subscribe("chat/room", new GrainId("ChatUser", "alice"));

      const stream = provider.getStream<string>("chat", "room");
      await stream.publish("hello");

      await waitFor(() => received.length === 1);
      expect(received).toEqual(["hello"]);
    } finally {
      provider.stop();
    }
  }, 10_000);

  it("delivers to implicit subscribers with no explicit subscription", async () => {
    const provider = new PostgresPullingStreamProvider(pool!, "implicit", {
      tablePrefix: prefix,
      queueCount: 1,
      pollIntervalMs: 5,
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
    }
  }, 10_000);

  it("resumes delivery from the committed cursor for a fresh provider instance (handoff)", async () => {
    const opts = { tablePrefix: prefix, queueCount: 1, pollIntervalMs: 5 };
    const first = new PostgresPullingStreamProvider(pool!, "handoff", opts);
    await first.start();
    const received: string[] = [];
    first.setDeliver(async (_s, _k, event) => void received.push(event as string));
    first.setImplicitSubscribers((ns) => (ns === "chat" ? ["Listener"] : []));

    first.startAgentsFor([0]);
    await first.getStream<string>("chat", "room").publish("m1");
    await waitFor(() => received.length === 1);
    first.stop();

    // A fresh provider instance (new agents) picks up from the durably
    // committed cursor: publishing more events before it starts must not be
    // lost, and the already-delivered event must not be redelivered.
    await first.getStream<string>("chat", "room").publish("m2");

    const second = new PostgresPullingStreamProvider(pool!, "handoff", opts);
    second.setDeliver(async (_s, _k, event) => void received.push(event as string));
    second.setImplicitSubscribers((ns) => (ns === "chat" ? ["Listener"] : []));
    try {
      second.startAgentsFor([0]);
      await waitFor(() => received.length === 2);
      expect(received).toEqual(["m1", "m2"]);
    } finally {
      second.stop();
    }
  }, 10_000);

  it("skips a poison event after exhausting retries and records it to the durable failure store", async () => {
    const failureStore = new PostgresStreamFailureStore(pool!, prefix);
    await failureStore.start();
    const failureHandler: StreamFailureHandler = new DurableStreamFailureHandler(failureStore);

    const provider = new PostgresPullingStreamProvider(pool!, "poison", {
      tablePrefix: prefix,
      queueCount: 1,
      pollIntervalMs: 5,
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
    }
  }, 10_000);

  it("registers and unregisters an explicit producer", async () => {
    const provider = new PostgresPullingStreamProvider(pool!, "producers", {
      tablePrefix: prefix,
      queueCount: 1,
    });
    await provider.start();

    const handle = await provider.registerProducer("chat", "room-1");
    expect(handle.streamId).toEqual({ provider: "producers", namespace: "chat", key: "room-1" });
    await handle.unregister();
    // Idempotent.
    await handle.unregister();
  });

  it("trims delivered rows on commit, honoring retainFor", async () => {
    const eager = new PostgresPullingStreamProvider(pool!, "trim-eager", {
      tablePrefix: prefix,
      queueCount: 1,
      pollIntervalMs: 5,
    });
    await eager.start();
    eager.setDeliver(async () => undefined);
    eager.setImplicitSubscribers((ns) => (ns === "chat" ? ["Listener"] : []));

    try {
      eager.startAgentsFor([0]);
      await eager.getStream<string>("chat", "room").publish("m1");
      await waitFor(async () => {
        const res = await pool!.query(
          `SELECT cursor FROM ${prefix}_cursors WHERE provider = 'trim-eager'`,
        );
        return res.rows.length === 1;
      });

      const res = await pool!.query(
        `SELECT id FROM ${prefix}_events WHERE provider = 'trim-eager'`,
      );
      expect(res.rows).toHaveLength(0);
    } finally {
      eager.stop();
    }
  }, 10_000);
});
