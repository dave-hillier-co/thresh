import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import { RedisStreamQueue } from "@thresh/streams/redis-stream-queue";

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
const prefix = `thresh-test:streamq:${randomUUID()}`;

afterAll(async () => {
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisStreamQueue commit", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("commits advance the cursor", async () => {
    const queue = new RedisStreamQueue(client!, `${prefix}:q`);
    expect(await queue.getCursor()).toBe(0);
    await queue.commit(5);
    expect(await queue.getCursor()).toBe(5);
  });

  // Ownership-handoff regression: the de-owned pulling agent's fire-and-forget
  // commit can land after the new owner (on another silo) has already
  // committed further ahead. A stale, smaller commit must not rewind the
  // cursor — that would redeliver a whole batch to the new owner.
  it("does not regress the cursor on a stale, smaller commit (ownership handoff)", async () => {
    const queue = new RedisStreamQueue(client!, `${prefix}:q`);
    await queue.commit(10);
    await queue.commit(4); // stale commit racing in from the de-owned agent
    expect(await queue.getCursor()).toBe(10);
  });

  it("still advances on a later, larger commit after a stale one was ignored", async () => {
    const queue = new RedisStreamQueue(client!, `${prefix}:q`);
    await queue.commit(10);
    await queue.commit(4);
    await queue.commit(15);
    expect(await queue.getCursor()).toBe(15);
  });

  // seek() is the deliberate escape hatch for RecoverableStreamDeliveryError's
  // checkpoint rewind — unlike commit(), it must go backwards on request.
  it("seek unconditionally rewinds the cursor", async () => {
    const queue = new RedisStreamQueue(client!, `${prefix}:q`);
    await queue.commit(10);
    await queue.seek(3);
    expect(await queue.getCursor()).toBe(3);
  });
});
