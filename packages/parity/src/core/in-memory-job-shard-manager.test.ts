// Ported from dotnet/orleans test/Orleans.Core.Tests/DurableJobs/InMemoryJobShardManagerTests.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { completed } from "@tsva/core/durable-job";
import { GrainId } from "@tsva/core/grain-id";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { MemoryJobShardStore } from "@tsva/durable-jobs/memory-job-shard-store";
import {
  LocalDurableJobManager,
  resolveOptions,
  type ShardOwnershipContext,
} from "@tsva/durable-jobs/local-durable-job-manager";

// Orleans' `InMemoryJobShardManager` is a SiloAddress/IClusterMembershipService-
// aware DI service backed by static in-process state; the "AssignJobShardsAsync"
// / "UnregisterShardAsync" API it exposes is split across two components here:
// `JobShardStore` (the durable claim/release/listShards primitives — a plain
// object, not a DI singleton keyed by SiloAddress) and `LocalDurableJobManager`
// (which reconciles ownership against a `ShardOwnershipContext` of ring-key
// strings, not `SiloAddress`/`IClusterMembershipService`). Tests below exercise
// the store directly where the upstream Fact is really about claim/adopt/poison
// semantics, and the manager where it is really about ownership reconciliation.
describe("NonSilo.Tests.DurableJobs.InMemoryJobShardManagerTests", () => {
  const NS = "NonSilo.Tests.DurableJobs.InMemoryJobShardManagerTests";

  orleansTest.excluded(
    "No explicit CreateShardAsync API exists: this framework derives a shard's " +
      "bounds from `shardKeyFor`/`shardStartMs` (job-model.ts) rather than creating and " +
      "persisting a ShardRecord with an explicit StartTime/EndTime up front — a shard " +
      "record is created lazily the first time a job is persisted under its key.",
    `${NS}.CreateShardAsync_CreatesShardOwnedBySilo`,
  );

  orleansTest(`${NS}.AssignJobShardsAsync_ReturnsOwnedShards`, async () => {
    const store = new MemoryJobShardStore();
    await store.persistAdd({
      id: "j1",
      name: "job",
      dueTime: new Date(1000),
      target: new GrainId("test", "grain1"),
      shardKey: 5,
      metadata: {},
    });

    const claimed = await store.claimShard(5, "silo1", { maxAdoptedCount: 3 });
    expect(claimed?.owner).toBe("silo1");

    const shards = await store.listShards();
    expect(shards).toHaveLength(1);
    expect(shards[0]?.shardKey).toBe(5);
  });

  orleansTest(
    `${NS}.AssignJobShardsAsync_OrphanedShard_IsAssignedWithoutIncrementingAdoptedCount`,
    async () => {
      const store = new MemoryJobShardStore();
      await store.persistAdd({
        id: "j1",
        name: "job",
        dueTime: new Date(1000),
        target: new GrainId("test", "grain1"),
        shardKey: 5,
        metadata: {},
      });

      await store.claimShard(5, "silo1", { maxAdoptedCount: 3 });
      // Silo1 gracefully releases (sets owner to undefined) — an orphan, not a
      // dead-owner adoption.
      await store.releaseShard(5, "silo1");

      const claimed = await store.claimShard(5, "silo2", { maxAdoptedCount: 3 });
      expect(claimed?.owner).toBe("silo2");
      expect(claimed?.adoptedCount).toBe(0);
    },
  );

  orleansTest(`${NS}.AssignJobShardsAsync_AdoptedFromDeadSilo_IncrementsAdoptedCount`, async () => {
    const store = new MemoryJobShardStore();
    await store.persistAdd({
      id: "j1",
      name: "job",
      dueTime: new Date(1000),
      target: new GrainId("test", "grain1"),
      shardKey: 5,
      metadata: {},
    });

    await store.claimShard(5, "silo1", { maxAdoptedCount: 3 });

    const claimed = await store.claimShard(5, "silo2", {
      adoptFromDead: ["silo1"],
      maxAdoptedCount: 3,
    });
    expect(claimed?.owner).toBe("silo2");
    expect(claimed?.adoptedCount).toBe(1);
  });

  orleansTest(`${NS}.AssignJobShardsAsync_PoisonedShard_IsNotAssigned`, async () => {
    const store = new MemoryJobShardStore();
    await store.persistAdd({
      id: "j1",
      name: "job",
      dueTime: new Date(1000),
      target: new GrainId("test", "grain1"),
      shardKey: 8,
      metadata: {},
    });

    await store.claimShard(8, "silo1", { maxAdoptedCount: 2 });
    let owner = "silo1";
    for (let i = 1; i <= 2; i++) {
      const next = `silo${i + 1}`;
      const claimed = await store.claimShard(8, next, {
        adoptFromDead: [owner],
        maxAdoptedCount: 2,
      });
      expect(claimed?.adoptedCount).toBe(i);
      owner = next;
    }

    // The 3rd adoption exceeds the limit of 2 and poisons the shard.
    const poisoned = await store.claimShard(8, "silo4", {
      adoptFromDead: [owner],
      maxAdoptedCount: 2,
    });
    expect(poisoned).toBeUndefined();
    // Poisoned, so no longer assignable even to a fresh, non-adopting claim.
    expect(await store.claimShard(8, "silo5", { maxAdoptedCount: 2 })).toBeUndefined();
  });

  orleansTest(
    `${NS}.AssignJobShardsAsync_MaxAdoptedCountOfZero_NeverAssignsAdoptedShards`,
    async () => {
      const store = new MemoryJobShardStore();
      await store.persistAdd({
        id: "j1",
        name: "job",
        dueTime: new Date(1000),
        target: new GrainId("test", "grain1"),
        shardKey: 5,
        metadata: {},
      });

      await store.claimShard(5, "silo1", { maxAdoptedCount: 0 });

      const claimed = await store.claimShard(5, "silo2", {
        adoptFromDead: ["silo1"],
        maxAdoptedCount: 0,
      });
      expect(claimed).toBeUndefined();
    },
  );

  orleansTest(`${NS}.UseInMemoryDurableJobs_ConfiguredMaxAdoptedCount_IsApplied`, async () => {
    // Mirrors the DI-wiring intent: `DurableJobsOptions.maxAdoptedCount` (as threaded
    // through `resolveOptions`, the same path `SiloBuilder.useMemoryDurableJobs`
    // uses) actually governs whether `LocalDurableJobManager.refreshOwnership`
    // adopts a dead-owner shard.
    const store = new MemoryJobShardStore();
    await store.persistAdd({
      id: "j1",
      name: "job",
      dueTime: new Date(1000),
      target: new GrainId("test", "grain1"),
      shardKey: 5,
      metadata: {},
    });
    await store.claimShard(5, "dead-silo", { maxAdoptedCount: 3 });

    const time = new FakeTimeProvider();
    const options = resolveOptions({ maxAdoptedCount: 0 });
    const ownership: ShardOwnershipContext = { localRingKey: "silo1", activeRingKeys: ["silo1"] };
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    await manager.refreshOwnership(ownership);

    expect(manager.ownedShards()).toEqual([]);
  });

  orleansTest(`${NS}.AssignJobShardsAsync_ShardFromActiveSilo_IsNotAssigned`, async () => {
    const store = new MemoryJobShardStore();
    await store.persistAdd({
      id: "j1",
      name: "job",
      dueTime: new Date(1000),
      target: new GrainId("test", "grain1"),
      shardKey: 5,
      metadata: {},
    });

    await store.claimShard(5, "silo1", { maxAdoptedCount: 3 });

    // silo1 is still alive (not passed as adoptFromDead): silo2 cannot take it.
    const claimed = await store.claimShard(5, "silo2", { maxAdoptedCount: 3 });
    expect(claimed).toBeUndefined();
  });

  orleansTest(`${NS}.UnregisterShardAsync_WithNoJobsRemaining_RemovesShard`, async () => {
    const store = new MemoryJobShardStore();
    await store.persistAdd({
      id: "j1",
      name: "job",
      dueTime: new Date(1000),
      target: new GrainId("test", "grain1"),
      shardKey: 5,
      metadata: {},
    });
    await store.claimShard(5, "silo1", { maxAdoptedCount: 3 });

    // No jobs remaining, then unregister (release).
    await store.persistRemove(5, "j1");
    await store.releaseShard(5, "silo1");

    // With no jobs left, the shard drops out of listShards — reconciliation
    // (the AssignJobShardsAsync equivalent) never reconsiders it.
    const time = new FakeTimeProvider();
    const options = resolveOptions({});
    const ownership: ShardOwnershipContext = { localRingKey: "silo2", activeRingKeys: ["silo2"] };
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );
    await manager.refreshOwnership(ownership);

    expect(manager.ownedShards()).toEqual([]);
  });

  orleansTest(`${NS}.UnregisterShardAsync_WithJobsRemaining_MarksShardAsOrphaned`, async () => {
    const store = new MemoryJobShardStore();
    await store.persistAdd({
      id: "j1",
      name: "job",
      dueTime: new Date(1000),
      target: new GrainId("test", "grain1"),
      shardKey: 5,
      metadata: {},
    });
    await store.claimShard(5, "silo1", { maxAdoptedCount: 3 });

    // Unregister (release) while a job remains: the shard is orphaned, not removed.
    await store.releaseShard(5, "silo1");

    const time = new FakeTimeProvider();
    const options = resolveOptions({});
    const ownership: ShardOwnershipContext = { localRingKey: "silo2", activeRingKeys: ["silo2"] };
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );
    await manager.refreshOwnership(ownership);

    expect(manager.ownedShards()).toEqual([5]);
  });
});
