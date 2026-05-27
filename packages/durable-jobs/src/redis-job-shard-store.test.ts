import { randomUUID } from "node:crypto";
import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { createClient } from "redis";
import type { DurableJob } from "@tsva/core/durable-job";
import { GrainId } from "@tsva/core/grain-id";
import { RedisJobShardStore } from "@tsva/durable-jobs/redis-job-shard-store";

const REDIS_URL = process.env.REDIS_URL ?? "redis://localhost:6379";

type Client = ReturnType<typeof createClient>;

/** Probe Redis once at load time so the suite skips cleanly without it. */
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

const client = await reachable(REDIS_URL);
const prefix = `tsva-test:job:${randomUUID()}`;
const makeStore = () => new RedisJobShardStore(client!, { keyPrefix: prefix });

function jobAt(id: string, shardKey: number, dueMs: number): DurableJob {
  return {
    id,
    name: "ok",
    dueTime: new Date(dueMs),
    target: new GrainId("Worker", "w"),
    shardKey,
    metadata: { userId: "u1" },
  };
}

async function deleteAll(c: Client, match: string): Promise<void> {
  for await (const keys of c.scanIterator({ MATCH: match })) {
    if (keys.length > 0) await c.del(keys);
  }
}

afterAll(async () => {
  if (client === undefined) return;
  await deleteAll(client, `${prefix}*`);
  await client.destroy();
});

describe.skipIf(client === undefined)("RedisJobShardStore", () => {
  beforeEach(async () => {
    await deleteAll(client!, `${prefix}*`);
  });

  it("persists and reads back a job through a fresh client", async () => {
    const job = jobAt("j1", 5, 100_000);
    await makeStore().persistAdd(job);

    const jobs = await makeStore().readJobs(5);
    expect(jobs).toHaveLength(1);
    expect(jobs[0]?.job.id).toBe("j1");
    expect(jobs[0]?.job.metadata).toEqual({ userId: "u1" });
    expect(jobs[0]?.dueTime.getTime()).toBe(100_000);
    expect(jobs[0]?.dequeueCount).toBe(0);
  });

  it("advances due time and dequeue count on retry, and removes on completion", async () => {
    const store = makeStore();
    await store.persistAdd(jobAt("j2", 5, 1000));
    await store.persistRetry(5, "j2", new Date(9000), 2);
    const [persisted] = await makeStore().readJobs(5);
    expect(persisted?.dueTime.getTime()).toBe(9000);
    expect(persisted?.dequeueCount).toBe(2);

    await store.persistRemove(5, "j2");
    expect(await makeStore().readJobs(5)).toEqual([]);
  });

  it("lists shards that have jobs", async () => {
    const store = makeStore();
    await store.persistAdd(jobAt("a", 1, 1000));
    await store.persistAdd(jobAt("b", 2, 1000));
    const shards = await makeStore().listShards();
    expect(shards.map((s) => s.shardKey).sort((x, y) => x - y)).toEqual([1, 2]);
  });

  it("claims a shard atomically: a second owner loses while the first holds it", async () => {
    const store = makeStore();
    await store.persistAdd(jobAt("a", 7, 1000));
    const first = await store.claimShard(7, "silo-a", { maxAdoptedCount: 3 });
    expect(first?.owner).toBe("silo-a");
    // A different live silo cannot steal it.
    const second = await store.claimShard(7, "silo-b", { maxAdoptedCount: 3 });
    expect(second).toBeUndefined();
    // The same owner re-claiming is idempotent.
    expect((await store.claimShard(7, "silo-a", { maxAdoptedCount: 3 }))?.owner).toBe("silo-a");
  });

  it("adopts a dead owner's shard, poisoning past the adoption limit", async () => {
    const store = makeStore();
    await store.persistAdd(jobAt("a", 8, 1000));
    await store.claimShard(8, "silo-dead", { maxAdoptedCount: 3 });

    // Adopt from the dead owner repeatedly; each adoption increments adoptedCount.
    let owner = "silo-dead";
    for (let i = 1; i <= 3; i++) {
      const next = `silo-${i}`;
      const claimed = await store.claimShard(8, next, {
        adoptFromDead: [owner],
        maxAdoptedCount: 3,
      });
      expect(claimed?.adoptedCount).toBe(i);
      owner = next;
    }
    // The 4th adoption exceeds the limit and poisons the shard.
    const poisoned = await store.claimShard(8, "silo-4", {
      adoptFromDead: [owner],
      maxAdoptedCount: 3,
    });
    expect(poisoned).toBeUndefined();
    // A poisoned shard is never claimable again, even when unowned.
    expect(await store.claimShard(8, "silo-5", { maxAdoptedCount: 3 })).toBeUndefined();
  });

  it("releases a shard only for its current owner", async () => {
    const store = makeStore();
    await store.persistAdd(jobAt("a", 9, 1000));
    await store.claimShard(9, "silo-a", { maxAdoptedCount: 3 });
    // A non-owner release is a no-op.
    await store.releaseShard(9, "silo-b");
    expect((await makeStore().listShards()).find((s) => s.shardKey === 9)?.owner).toBe("silo-a");
    // The owner can release, freeing it for a successor.
    await store.releaseShard(9, "silo-a");
    expect((await makeStore().listShards()).find((s) => s.shardKey === 9)?.owner).toBeUndefined();
  });
});
