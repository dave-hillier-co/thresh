import { randomUUID } from "node:crypto";
import { describe, expect, it } from "vitest";
import { createClient } from "redis";
import type { StreamFailureHandler } from "@thresh/streams/queue-pulling-agent";
import { RedisPullingStreamProvider } from "@thresh/streams/redis-pulling-stream-provider";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

async function reachable(url: string): Promise<Client | undefined> {
  const probe = createClient({ url });
  probe.on("error", () => {});
  try {
    await probe.connect();
    await probe.ping();
    return probe;
  } catch {
    try {
      await probe.destroy();
    } catch {
      /* never connected */
    }
    return undefined;
  }
}

async function waitFor(predicate: () => boolean, timeoutMs = 4000): Promise<void> {
  const start = Date.now();
  while (!predicate()) {
    if (Date.now() - start > timeoutMs) throw new Error("waitFor: timed out");
    await new Promise((r) => setTimeout(r, 10));
  }
}

const client = await reachable(REDIS_URL);
const prefix = `thresh-test:redis-pulling:${randomUUID()}`;

describe.skipIf(client === undefined)("RedisPullingStreamProvider", () => {
  it("forwards the failureHandler option to every queue's pulling agent", async () => {
    const failures: Array<{ streamKey: string; attempts: number }> = [];
    const failureHandler: StreamFailureHandler = {
      async onDeliveryFailure(streamKey, _event, _token, _error, attempts) {
        failures.push({ streamKey, attempts });
      },
    };

    const provider = new RedisPullingStreamProvider(client!, "failure-handler", {
      keyPrefix: prefix,
      queueCount: 1,
      pollIntervalMs: 5,
      failureHandler,
    });
    provider.setDeliver(async () => {
      throw new Error("always fails");
    });
    provider.setImplicitSubscribers((namespace) => (namespace === "room" ? ["poison-grain"] : []));

    try {
      provider.startAgentsFor([0]);
      const stream = provider.getStream<string>("room", "bad");
      await stream.publish("poison");

      await waitFor(() => failures.length === 1);
      expect(failures).toEqual([{ streamKey: "room/bad", attempts: 3 }]);
    } finally {
      provider.stop();
    }
  }, 10_000);

  it("does not forward a failure when no failureHandler is configured", async () => {
    const provider = new RedisPullingStreamProvider(client!, "no-failure-handler", {
      keyPrefix: prefix,
      queueCount: 1,
      pollIntervalMs: 5,
    });
    let attempts = 0;
    provider.setDeliver(async () => {
      attempts++;
      throw new Error("always fails");
    });
    provider.setImplicitSubscribers((namespace) => (namespace === "room" ? ["poison-grain"] : []));

    try {
      provider.startAgentsFor([0]);
      const stream = provider.getStream<string>("room", "bad2");
      await stream.publish("poison");

      await waitFor(() => attempts >= 3);
      // Nothing should throw from the agent even with no handler wired.
      await new Promise((r) => setTimeout(r, 50));
    } finally {
      provider.stop();
    }
  }, 10_000);

  it("registers and unregisters an explicit producer", async () => {
    const provider = new RedisPullingStreamProvider(client!, "producers", {
      keyPrefix: prefix,
      queueCount: 1,
    });

    const handle = await provider.registerProducer("chat", "room-1");
    expect(handle.streamId).toEqual({ provider: "producers", namespace: "chat", key: "room-1" });
    await handle.unregister();
    // Idempotent.
    await handle.unregister();
  });
});
