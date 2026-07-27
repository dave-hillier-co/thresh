import { describe, expect, it } from "vitest";
import { completed } from "@thresh/core/durable-job";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import {
  LocalDurableJobManager,
  resolveOptions,
  type ShardOwnershipContext,
} from "@thresh/durable-jobs/local-durable-job-manager";

/**
 * Persist and orphan-claim `count` shards (owned by a dead silo, so a fresh
 * manager can adopt them). The job's due time is far in the future so the
 * executor's own poll loop does not run and remove it mid-test — `listShards`
 * drops an empty shard, which would otherwise make an adopted shard vanish
 * from `ownedShards()` on its own, independent of ramp-up.
 */
async function seedOrphanedShards(store: MemoryJobShardStore, count: number): Promise<void> {
  for (let i = 0; i < count; i++) {
    await store.persistAdd({
      id: `j${i}`,
      name: "job",
      dueTime: new Date(1_000_000_000),
      target: new GrainId("test", `grain${i}`),
      shardKey: i,
      metadata: {},
    });
    await store.claimShard(i, "dead-silo", { maxAdoptedCount: 3 });
  }
}

describe("LocalDurableJobManager.refreshOwnership time-based claim ramp-up", () => {
  const ownership: ShardOwnershipContext = { localRingKey: "silo1", activeRingKeys: ["silo1"] };

  it("with ramp-up disabled (the default), claims are unlimited by time — only the flat claimRampUpBudget applies", async () => {
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 10);

    const time = new FakeTimeProvider();
    const options = resolveOptions({ claimRampUpBudget: 4 });
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    await manager.refreshOwnership(ownership);

    // Unchanged legacy behaviour: bounded by the flat per-step budget, not by elapsed time.
    expect(manager.ownedShards()).toHaveLength(4);
  });

  it("a freshly joined silo claims at most the interpolated budget early in the ramp-up window", async () => {
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 10);

    const time = new FakeTimeProvider();
    const options = resolveOptions({
      shardClaimInitialBudget: 2,
      shardClaimMaxBudget: 20,
      shardClaimRampUpDuration: { minutes: 5 },
    });
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    // No time has elapsed since the manager was constructed (join time): initial budget only.
    await manager.refreshOwnership(ownership);

    expect(manager.ownedShards()).toHaveLength(2);
  });

  it("the budget grows as time elapses since join", async () => {
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 20);

    const time = new FakeTimeProvider();
    const options = resolveOptions({
      shardClaimInitialBudget: 2,
      shardClaimMaxBudget: 20,
      shardClaimRampUpDuration: { minutes: 5 },
    });
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    await manager.refreshOwnership(ownership);
    expect(manager.ownedShards()).toHaveLength(2);

    // Halfway through the ramp-up window: 2 + 0.5 * (20 - 2) = 11 total budget,
    // minus the 2 already claimed this window = 9 more.
    time.advance(2.5 * 60_000);
    await manager.refreshOwnership(ownership);
    expect(manager.ownedShards()).toHaveLength(11);
  });

  it("cumulative shards claimed within the window are subtracted from the interpolated budget", async () => {
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 3);

    const time = new FakeTimeProvider();
    const options = resolveOptions({
      shardClaimInitialBudget: 2,
      shardClaimMaxBudget: 20,
      shardClaimRampUpDuration: { minutes: 5 },
    });
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    await manager.refreshOwnership(ownership);
    // Only 2 of the 3 available orphaned shards were claimable under the initial budget.
    expect(manager.ownedShards()).toHaveLength(2);

    // Still at time 0 (no elapsed time): the budget is still 2, all already spent.
    await manager.refreshOwnership(ownership);
    expect(manager.ownedShards()).toHaveLength(2);
  });

  it("after the ramp-up window elapses, claiming is unlimited", async () => {
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 50);

    const time = new FakeTimeProvider();
    const options = resolveOptions({
      shardClaimInitialBudget: 2,
      shardClaimMaxBudget: 20,
      shardClaimRampUpDuration: { minutes: 5 },
    });
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    time.advance(5 * 60_000);
    await manager.refreshOwnership(ownership);

    expect(manager.ownedShards()).toHaveLength(50);
  });

  it("with ramp-up disabled explicitly (zero duration), claiming falls back to the flat claimRampUpBudget", async () => {
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 10);

    const time = new FakeTimeProvider();
    const options = resolveOptions({
      shardClaimRampUpDuration: { ms: 0 },
      claimRampUpBudget: 3,
    });
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    await manager.refreshOwnership(ownership);

    expect(manager.ownedShards()).toHaveLength(3);
  });
});
