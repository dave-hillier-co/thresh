import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";

async function storeWithJob(): Promise<MemoryJobShardStore> {
  const store = new MemoryJobShardStore();
  await store.persistAdd({
    id: "j",
    name: "job",
    dueTime: new Date(1_000_000_000),
    target: new GrainId("test", "g"),
    shardKey: 1,
    metadata: {},
  });
  return store;
}

describe("MemoryJobShardStore adoption count", () => {
  it("never poisons a shard that is adopted and then gracefully released, however often", async () => {
    const store = await storeWithJob();
    for (let i = 0; i < 10; i++) {
      await store.claimShard(1, "dead", { maxAdoptedCount: 3 });
      const adopted = await store.claimShard(1, "live", {
        adoptFromDead: ["dead"],
        maxAdoptedCount: 3,
      });
      expect(adopted?.poisoned).toBe(false);
      await store.releaseShard(1, "live");
    }
    const [record] = await store.listShards();
    expect(record).toMatchObject({ adoptedCount: 0, poisoned: false });
  });

  it("poisons a shard adopted from dead owners past the maximum and reports it on the claim", async () => {
    const store = await storeWithJob();
    let last;
    for (let i = 0; i < 4; i++) {
      await store.claimShard(1, `dead${i}`, { maxAdoptedCount: 3 });
      last = await store.claimShard(1, `dead${i + 1}`, {
        adoptFromDead: [`dead${i}`],
        maxAdoptedCount: 3,
      });
    }
    expect(last?.poisoned).toBe(true);
    expect((await store.listShards())[0]?.poisoned).toBe(true);
    // Once poisoned it is never claimed again, and is not re-reported.
    expect(await store.claimShard(1, "x", { maxAdoptedCount: 3 })).toBeUndefined();
  });
});
