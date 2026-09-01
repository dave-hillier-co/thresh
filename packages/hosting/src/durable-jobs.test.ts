import { describe, expect, it } from "vitest";
import { defineGrain, useDurableJobHandler } from "@thresh/core/define-grain";
import { completed, pollAfter, type DurableJob } from "@thresh/core/durable-job";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainKey } from "@thresh/core/key-kinds";
import { PLACEMENT_HINT_KEY, RequestContext } from "@thresh/core/request-context";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createSilo } from "@thresh/hosting/silo-builder";

// A module sink keeps observable run records across (re)activations and restarts —
// the grain activation itself may be fresh.
const runs = new Map<string, number>();
const pollAttempts = new Map<string, number>();
const record = (key: string, map = runs) => map.set(key, (map.get(key) ?? 0) + 1);

interface IWorker extends GrainKey<string> {
  ping(): Promise<string>;
}
const IWorker = defineGrainInterface<IWorker>("IWorker.jobs");

// A worker grain whose durable-job handler is driven by per-name behaviour:
//  - "ok"      → record a run and complete
//  - "boom"    → record a run and throw (Failed → retry policy)
//  - "poll"    → poll a few times then complete (supervised re-poll)
const WorkerGrain = defineGrain<IWorker>("Worker", (ctx) => {
  useDurableJobHandler(async (job) => {
    const key = `${String(ctx.id.key)}:${job.name}`;
    if (job.name === "boom") {
      record(key);
      throw new Error("boom");
    }
    if (job.name === "poll") {
      const n = (pollAttempts.get(key) ?? 0) + 1;
      pollAttempts.set(key, n);
      if (n < 3) return pollAfter({ seconds: 5 });
      record(key);
      return completed;
    }
    record(key);
    return completed;
  });
  return { ping: async () => "pong" };
});

// A scheduler grain that schedules/cancels jobs on a worker via the runtime.
interface IScheduler extends GrainKey<string> {
  schedule(workerKey: string, name: string, dueMs: number): Promise<DurableJob>;
  cancel(job: DurableJob): Promise<void>;
}
const IScheduler = defineGrainInterface<IScheduler>("IScheduler.jobs");

const SchedulerGrain = defineGrain<IScheduler>("Scheduler", (ctx) => ({
  schedule: async (workerKey, name, dueMs) =>
    ctx.runtime.scheduleJob({
      target: new GrainId("Worker", workerKey),
      name,
      dueTime: new Date(dueMs),
    }),
  cancel: async (job) => ctx.runtime.cancelJob(job),
}));

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

/** Let queued microtasks (the awaited dispatch + result application) settle. */
async function flush(times = 5): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 0));
}

/**
 * Drive the fake clock forward in small steps, flushing microtasks between each,
 * so timers that are (re)armed asynchronously inside a job's result application
 * (retry backoff, pollAfter re-poll) come due and fire. A single big `advance`
 * would miss them, since the next timer is only set after the current run settles.
 */
async function advanceSettling(
  time: FakeTimeProvider,
  totalMs: number,
  stepMs = 1000,
): Promise<void> {
  let elapsed = 0;
  while (elapsed < totalMs) {
    const step = Math.min(stepMs, totalMs - elapsed);
    time.advance(step);
    elapsed += step;
    await flush();
  }
}

function buildSilo(store: MemoryJobShardStore, time: FakeTimeProvider) {
  return createSilo({ clusterId: "c1", local, time })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryDurableJobs(store)
    .registerGrain(WorkerGrain.grain, { interfaces: [IWorker] })
    .registerGrain(SchedulerGrain.grain, { interfaces: [IScheduler] })
    .build();
}

describe("durable jobs end-to-end", () => {
  it("fires a scheduled job as a turn on the target at its due time, then removes it", async () => {
    runs.clear();
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const silo = buildSilo(store, time);
    await silo.start();
    try {
      await silo.getGrain(IScheduler, "s").schedule("w1", "ok", time.now() + 10_000);
      // Not due yet.
      await flush();
      expect(runs.get("w1:ok") ?? 0).toBe(0);

      time.advance(10_000);
      await flush();
      expect(runs.get("w1:ok")).toBe(1);

      // Completed jobs are removed from the store.
      const shardKey = Math.floor(10_000 / 3_600_000);
      expect(await store.readJobs(shardKey)).toEqual([]);
    } finally {
      await silo.stop();
    }
  });

  it("retries a throwing job with backoff, then drops it after the retry budget", async () => {
    runs.clear();
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const silo = buildSilo(store, time);
    await silo.start();
    try {
      await silo.getGrain(IScheduler, "s").schedule("w2", "boom", time.now() + 1000);
      time.advance(1000);
      await flush();
      expect(runs.get("w2:boom")).toBe(1); // first attempt

      // Default policy: 2^n s backoff (2,4,8,16s). Step the clock so each rearmed
      // retry timer fires; after the budget the job is dropped.
      await advanceSettling(time, 60_000);
      // 5 total attempts (default maxAttempts), then dropped.
      expect(runs.get("w2:boom")).toBe(5);
      expect(await store.readJobs(0)).toEqual([]); // dropped from the store
    } finally {
      await silo.stop();
    }
  });

  it("re-polls a pollAfter job, holding it until it completes", async () => {
    runs.clear();
    pollAttempts.clear();
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const silo = buildSilo(store, time);
    await silo.start();
    try {
      await silo.getGrain(IScheduler, "s").schedule("w3", "poll", time.now() + 1000);
      time.advance(1000);
      await flush();
      expect(pollAttempts.get("w3:poll")).toBe(1);
      expect(runs.get("w3:poll") ?? 0).toBe(0); // not done; re-polling

      time.advance(5000);
      await flush();
      expect(pollAttempts.get("w3:poll")).toBe(2);

      time.advance(5000);
      await flush();
      expect(runs.get("w3:poll")).toBe(1); // third poll completed
      expect(await store.readJobs(0)).toEqual([]);
    } finally {
      await silo.stop();
    }
  });

  it("cancels a pending job before it fires", async () => {
    runs.clear();
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const silo = buildSilo(store, time);
    await silo.start();
    try {
      const job = await silo.getGrain(IScheduler, "s").schedule("w4", "ok", time.now() + 10_000);
      await silo.getGrain(IScheduler, "s").cancel(job);
      time.advance(10_000);
      await flush();
      expect(runs.get("w4:ok") ?? 0).toBe(0);
      expect(await store.readJobs(0)).toEqual([]);
    } finally {
      await silo.stop();
    }
  });

  it("fires a job scheduled before a silo restart, after the restart (shared store)", async () => {
    runs.clear();
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();

    const first = buildSilo(store, time);
    await first.start();
    await first.getGrain(IScheduler, "s").schedule("w5", "ok", time.now() + 100_000);
    await first.stop(); // pod dies before the job is due

    const restarted = buildSilo(store, time); // new pod, same durable store
    await restarted.start();
    try {
      time.advance(100_000);
      await flush();
      expect(runs.get("w5:ok")).toBe(1);
    } finally {
      await restarted.stop();
    }
  });

  it("throws if a grain schedules a job on a silo without durable jobs configured", async () => {
    const silo = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(WorkerGrain.grain, { interfaces: [IWorker] })
      .registerGrain(SchedulerGrain.grain, { interfaces: [IScheduler] })
      .build();
    await silo.start();
    try {
      await expect(silo.getGrain(IScheduler, "s").schedule("w", "ok", 1000)).rejects.toThrow(
        /durable jobs/,
      );
    } finally {
      await silo.stop();
    }
  });
});

describe("durable jobs multi-silo failover", () => {
  // Two in-process silos sharing one membership view and one durable store. Killing
  // the silo that owns a shard lets the survivor adopt it and still run the job.
  const a = new SiloAddress("silo-a", "uid-a", "silo-a:1");
  const b = new SiloAddress("silo-b", "uid-b", "silo-b:2");

  it("adopts a dead owner's shard and still runs its jobs", async () => {
    runs.clear();
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const net = new InProcessNetwork();
    const membership = new StaticMembershipService(a, [a, b]);
    const membershipB = new StaticMembershipService(b, [a, b]);

    const buildAt = (addr: SiloAddress, ms: StaticMembershipService) =>
      createSilo({ clusterId: "c1", local: addr, time })
        .useMembership(ms)
        .useInProcessTransport(net)
        .useMemoryDurableJobs(store)
        .registerGrain(WorkerGrain.grain, { interfaces: [IWorker] })
        .registerGrain(SchedulerGrain.grain, { interfaces: [IScheduler] })
        .build();

    const siloA = buildAt(a, membership);
    const siloB = buildAt(b, membershipB);
    await siloA.start();
    await siloB.start();

    // Schedule a far-future job, then take down whichever silo owns its shard.
    const job = await siloA.getGrain(IScheduler, "s").schedule("wf", "ok", time.now() + 100_000);
    await flush();

    // Kill silo A (its shards, if any, orphan); narrow membership to B only.
    await siloA.stop();
    membershipB.setSilos([b]);
    await flush();
    // Nudge B's ownership reconciliation (the host also does this on a view change).
    time.advance(100_000);
    await flush(10);

    try {
      // Regardless of which silo originally owned the shard, the job runs exactly once.
      expect(runs.get("wf:ok")).toBe(1);
      expect(job.name).toBe("ok");
    } finally {
      await siloB.stop();
    }
  });

  it("delivers a job scheduled from a silo that does not own the target time-shard", async () => {
    runs.clear();
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const net = new InProcessNetwork();
    const membership = new StaticMembershipService(a, [a, b]);
    const membershipB = new StaticMembershipService(b, [a, b]);

    const buildAt = (addr: SiloAddress, ms: StaticMembershipService) =>
      createSilo({ clusterId: "c1", local: addr, time })
        .useMembership(ms)
        .useInProcessTransport(net)
        .useMemoryDurableJobs(store)
        .registerGrain(WorkerGrain.grain, { interfaces: [IWorker] })
        .registerGrain(SchedulerGrain.grain, { interfaces: [IScheduler] })
        .build();

    const siloA = buildAt(a, membership);
    const siloB = buildAt(b, membershipB);
    await siloA.start();
    await siloB.start();

    try {
      // Pin "s1"'s activation to silo A and schedule a job there: A claims and
      // owns that job's time shard.
      RequestContext.set(PLACEMENT_HINT_KEY, a.toString());
      const dueMs = time.now() + 100_000;
      await siloA.getGrain(IScheduler, "s1").schedule("owned-by-a", "ok", dueMs);
      await flush();

      // Pin a second scheduler activation to silo B — the non-owner of that
      // shard — and schedule a job due in the very same shard from there.
      RequestContext.set(PLACEMENT_HINT_KEY, b.toString());
      await siloB.getGrain(IScheduler, "s2").schedule("scheduled-from-b", "ok", dueMs);
      await flush();

      time.advance(100_000);
      await flush(10);

      // Both jobs fire even though the second was scheduled from the silo
      // that never owned (and never claimed) the shard.
      expect(runs.get("owned-by-a:ok")).toBe(1);
      expect(runs.get("scheduled-from-b:ok")).toBe(1);
    } finally {
      RequestContext.clear();
      await siloA.stop();
      await siloB.stop();
    }
  });
});
