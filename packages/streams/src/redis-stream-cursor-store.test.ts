import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { RedisStreamCursorStore } from "@thresh/streams/redis-stream-cursor-store";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";
type Client = ReturnType<typeof createClient>;

async function reachable(url: string): Promise<Client | undefined> {
  // `reconnectStrategy: false` is load-bearing, not tidiness: node-redis retries a refused
  // connection forever by default, so `connect()` below never settles when no Redis is
  // listening and this module-load probe hangs the whole suite instead of skipping it.
  const probe = createClient({ url, socket: { reconnectStrategy: false, connectTimeout: 500 } });
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

async function deleteAll(c: Client, match: string): Promise<void> {
  for await (const keys of c.scanIterator({ MATCH: match })) {
    if (keys.length > 0) await c.del(keys);
  }
}

const client = await reachable(REDIS_URL);
const prefix = `thresh-test:streamq-cursor:${randomUUID()}`;

afterAll(async () => {
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisStreamCursorStore", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("starts at cursor 0 and commits advance it", async () => {
    const store = new RedisStreamCursorStore(client!, prefix);
    expect(await store.getCursor("p", 0)).toBe(0);
    await store.commit("p", 0, 5);
    expect(await store.getCursor("p", 0)).toBe(5);
  });

  // Ownership-handoff regression: a stale commit from a de-owned pulling
  // agent racing the new owner's fresher commit must not rewind the cursor
  // and cause a whole batch to be redelivered.
  it("does not regress the cursor on a stale, smaller commit (ownership handoff)", async () => {
    const store = new RedisStreamCursorStore(client!, prefix);
    await store.commit("p", 0, 10);
    await store.commit("p", 0, 4);
    expect(await store.getCursor("p", 0)).toBe(10);
  });

  // seek() is the deliberate escape hatch for RecoverableStreamDeliveryError's
  // checkpoint rewind — unlike commit(), it must go backwards on request.
  it("seek unconditionally rewinds the cursor", async () => {
    const store = new RedisStreamCursorStore(client!, prefix);
    await store.commit("p", 0, 10);
    await store.seek("p", 0, 3);
    expect(await store.getCursor("p", 0)).toBe(3);
  });
});

// Issue #64: RedisStreamCursorStore partitions only by keyPrefix, so two
// services sharing one Redis silently share committed cursors. Redis has no
// ALTER, so this is a deliberate upgrade break: a cursor written under the
// old key shape orphans, resetting the queue to 0 for a default-configured
// reader.
describe.skipIf(client === undefined)(
  "RedisStreamCursorStore service partitioning (issue #64)",
  () => {
    beforeEach(async () => {
      await deleteAll(client!, `${prefix}*`);
    });

    it("isolates cursors by service id sharing prefix, provider and queue", async () => {
      const alpha = new RedisStreamCursorStore(client!, prefix, "alpha");
      const beta = new RedisStreamCursorStore(client!, prefix, "beta");
      await alpha.commit("p", 0, 42);
      expect(await beta.getCursor("p", 0)).toBe(0);
      await beta.commit("p", 0, 7);
      expect(await alpha.getCursor("p", 0)).toBe(42);
    });

    it("orphans a cursor written under the pre-service-dimension key shape (deliberate upgrade break)", async () => {
      // Old shape had no {serviceId} segment: `${prefix}:streamq:{provider}:{idx}:cursor`.
      await client!.set(`${prefix}:streamq:p:0:cursor`, "42");

      const defaultConfigured = new RedisStreamCursorStore(client!, prefix);
      expect(await defaultConfigured.getCursor("p", 0)).toBe(0);
    });
  },
);
