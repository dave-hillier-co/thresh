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

async function flush(times = 8): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 0));
}

describe("LocalDurableJobManager periodic shard check", () => {
  const ownership: ShardOwnershipContext = { localRingKey: "silo1", activeRingKeys: ["silo1"] };

  it("retries orphaned shards beyond the claim budget in a stable cluster (no membership change)", async () => {
    // A dead silo owned 10 shards; the single survivor can only claim 4 per
    // step (the ramp-up budget). Without a periodic re-check, the other 6
    // stay stranded until the next membership view change — which never
    // comes in a cluster that stays stable.
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 10);

    const time = new FakeTimeProvider();
    const options = resolveOptions({
      claimRampUpBudget: 4,
      periodicShardCheckInterval: { ms: 1000 },
    });
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    await manager.refreshOwnership(ownership); // the initial view-change reconcile
    expect(manager.ownedShards()).toHaveLength(4);

    time.advance(1000); // periodic shard check fires, no membership change
    await flush();
    expect(manager.ownedShards()).toHaveLength(8); // claims another budget's worth

    time.advance(1000);
    await flush();
    expect(manager.ownedShards()).toHaveLength(10); // the last 2 orphaned shards

    await manager.stop();
  });

  it("periodicShardCheckInterval: { ms: 0 } disables the periodic check", async () => {
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 10);

    const time = new FakeTimeProvider();
    const options = resolveOptions({
      claimRampUpBudget: 4,
      periodicShardCheckInterval: { ms: 0 },
    });
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    await manager.refreshOwnership(ownership);
    expect(manager.ownedShards()).toHaveLength(4);

    time.advance(10 * 60_000);
    await flush();
    expect(manager.ownedShards()).toHaveLength(4); // no periodic re-check ran

    await manager.stop();
  });
});

/** A shard store whose `listShards` can be made to fail, or to wait on a gate, and counts overlapping calls. */
class ControllableShardStore extends MemoryJobShardStore {
  failures = 0;
  gate: Promise<void> | undefined;
  listCalls = 0;
  inFlight = 0;
  maxInFlight = 0;

  override async listShards(): ReturnType<MemoryJobShardStore["listShards"]> {
    this.listCalls += 1;
    this.inFlight += 1;
    this.maxInFlight = Math.max(this.maxInFlight, this.inFlight);
    try {
      if (this.gate !== undefined) await this.gate;
      if (this.failures > 0) {
        this.failures -= 1;
        throw new Error("store unavailable");
      }
      return await super.listShards();
    } finally {
      this.inFlight -= 1;
    }
  }
}

describe("LocalDurableJobManager periodic shard check robustness", () => {
  const ownership: ShardOwnershipContext = { localRingKey: "silo1", activeRingKeys: ["silo1"] };
  const options = resolveOptions({
    claimRampUpBudget: 4,
    periodicShardCheckInterval: { ms: 1000 },
  });

  it("keeps checking after a check fails on a store error", async () => {
    // Orleans' PeriodicShardCheck logs and carries on after an error; a
    // failed check must not end the periodic re-check for good.
    const store = new ControllableShardStore();
    await seedOrphanedShards(store, 10);
    const time = new FakeTimeProvider();
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );

    store.failures = 1;
    await expect(manager.refreshOwnership(ownership)).rejects.toThrow("store unavailable");
    expect(manager.ownedShards()).toHaveLength(0);

    time.advance(1000);
    await flush();
    expect(manager.ownedShards()).toHaveLength(4);

    store.failures = 1; // the next periodic check fails too...
    time.advance(1000);
    await flush();
    expect(manager.ownedShards()).toHaveLength(4);

    time.advance(1000); // ...and the one after still runs
    await flush();
    expect(manager.ownedShards()).toHaveLength(8);

    await manager.stop();
  });

  it("stop() during an in-flight periodic check claims nothing and leaves no check armed", async () => {
    const store = new ControllableShardStore();
    await seedOrphanedShards(store, 10);
    const time = new FakeTimeProvider();
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );
    await manager.refreshOwnership(ownership);
    expect(manager.ownedShards()).toHaveLength(4);

    let release: () => void = () => undefined;
    store.gate = new Promise<void>((r) => {
      release = r;
    });
    time.advance(1000); // the periodic check starts and blocks in listShards
    await flush();
    const stopping = manager.stop();
    store.gate = undefined;
    release();
    await stopping;
    await flush();

    expect(manager.ownedShards()).toHaveLength(0); // no shard claimed after shutdown
    const callsAfterStop = store.listCalls;
    time.advance(10_000);
    await flush();
    expect(store.listCalls).toBe(callsAfterStop); // and no check re-armed
  });

  it("never runs a periodic check concurrently with a membership-driven one", async () => {
    // Before the periodic check, refreshOwnership only ran from the
    // sequential membership watch; the timer must not overlap it (an
    // overlapping run can stop an executor the other just started).
    const store = new ControllableShardStore();
    await seedOrphanedShards(store, 10);
    const time = new FakeTimeProvider();
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      options,
      ownership,
    );
    await manager.refreshOwnership(ownership);

    let release: () => void = () => undefined;
    store.gate = new Promise<void>((r) => {
      release = r;
    });
    store.maxInFlight = 0;
    time.advance(1000); // periodic check blocks in listShards
    await flush();
    const viewChange = manager.refreshOwnership(ownership); // membership change meanwhile
    await flush();
    store.gate = undefined;
    release();
    await viewChange;
    await flush();

    expect(store.maxInFlight).toBe(1);
    expect(manager.ownedShards()).toHaveLength(10);
    await manager.stop();
  });
});

describe("LocalDurableJobManager.stop draining", () => {
  it("drains an in-flight run before releasing its shard (undrained-stop regression)", async () => {
    // Ownership-handoff hazard: releasing the shard before the handler has
    // settled would let a successor claim it and re-run the same job while
    // the old owner's handler is still executing.
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const ownership: ShardOwnershipContext = { localRingKey: "silo1", activeRingKeys: ["silo1"] };

    let releaseRun: () => void = () => undefined;
    let running = false;
    const events: string[] = [];

    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => {
        running = true;
        await new Promise<void>((r) => {
          releaseRun = r;
        });
        running = false;
        events.push("handler-done");
        return completed;
      },
      resolveOptions({}),
      ownership,
    );

    const originalReleaseShard = store.releaseShard.bind(store);
    store.releaseShard = async (shardKey: number, owner: string) => {
      events.push("released");
      return originalReleaseShard(shardKey, owner);
    };

    await manager.scheduleJob({
      name: "job",
      dueTime: new Date(time.now()),
      target: new GrainId("test", "g"),
    });

    time.advance(0);
    await flush();
    expect(running).toBe(true); // the handler is mid-flight

    const stopping = manager.stop();
    await flush();
    expect(events).toEqual([]); // neither the handler nor the release has happened yet

    releaseRun();
    await stopping;

    expect(events).toEqual(["handler-done", "released"]);
  });
});

describe("LocalDurableJobManager after stop()", () => {
  const ownership: ShardOwnershipContext = { localRingKey: "silo1", activeRingKeys: ["silo1"] };

  // `SiloHost.stop` stops the manager BEFORE deactivating activations (Orleans
  // stops `LocalDurableJobManager` at `ServiceLifecycleStage.Active`), so the
  // grace period and every `onDeactivate` hook still run afterwards and may
  // schedule jobs, and a membership view change may still be mid-flight. None
  // of that may claim a shard and start an executor on this stopping silo:
  // nothing would ever stop it or release its shard again.
  it("persists a newly scheduled job without claiming its shard or starting an executor", async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      resolveOptions({}),
      ownership,
    );
    await manager.stop();

    const job = await manager.scheduleJob({
      name: "job",
      dueTime: new Date(time.now() + 60_000),
      target: new GrainId("test", "g"),
    });

    expect(manager.ownedShards()).toEqual([]);
    const shard = (await store.listShards()).find((s) => s.shardKey === job.shardKey);
    expect(shard).toBeDefined(); // persisted, for a live silo to claim
    expect(shard!.owner).toBeUndefined();
  });

  it("neither claims shards on an ownership refresh nor accepts a forwarded job", async () => {
    const store = new MemoryJobShardStore();
    await seedOrphanedShards(store, 2);
    const time = new FakeTimeProvider();
    const manager = new LocalDurableJobManager(
      store,
      time,
      async () => completed,
      resolveOptions({}),
      ownership,
    );
    await manager.stop();

    await manager.refreshOwnership(ownership);
    expect(manager.ownedShards()).toEqual([]);

    const accepted = await manager.receiveForwardedJob({
      id: "fwd",
      name: "job",
      dueTime: new Date(1_000_000_000),
      target: new GrainId("test", "g"),
      shardKey: 99,
      metadata: {},
    });
    expect(accepted).toBe(false);
    expect(manager.ownedShards()).toEqual([]);
  });
});
