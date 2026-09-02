import { describe, expect, it } from "vitest";
import {
  completed,
  pollAfter,
  type DurableJob,
  type JobRunContext,
} from "@thresh/core/durable-job";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { defaultShouldRetry } from "@thresh/durable-jobs/job-model";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import {
  ConcurrencyLimiter,
  ShardExecutor,
  type RunJob,
} from "@thresh/durable-jobs/shard-executor";

const OPTIONS = {
  shardDurationMs: 3_600_000,
  maxConcurrentJobsPerSilo: 2,
  slowStartInitialConcurrency: 1,
  slowStartGrowthFactor: 2,
  // Large enough that none of these tests' brief FakeTimeProvider advances
  // cross an interval boundary — slow-start growth is not under test here.
  slowStartIntervalMs: 1_000_000,
  overloadBackoffMs: 1000,
  shouldRetry: defaultShouldRetry,
};

function job(id: string, dueMs: number): DurableJob {
  return {
    id,
    name: "ok",
    dueTime: new Date(dueMs),
    target: new GrainId("W", "w"),
    shardKey: 0,
    metadata: {},
  };
}

async function flush(times = 8): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 0));
}

describe("ConcurrencyLimiter", () => {
  it("caps concurrent acquisitions and tracks saturation", () => {
    const limiter = new ConcurrencyLimiter(2);
    expect(limiter.tryAcquire()).toBe(true);
    expect(limiter.tryAcquire()).toBe(true);
    expect(limiter.saturated).toBe(true);
    expect(limiter.tryAcquire()).toBe(false);
    limiter.release();
    expect(limiter.saturated).toBe(false);
    expect(limiter.tryAcquire()).toBe(true);
  });
});

describe("ShardExecutor", () => {
  it("runs a due job as a completion and removes it from the store", async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(2);
    const ran: string[] = [];
    const run: RunJob = async (j) => {
      ran.push(j.id);
      return completed;
    };
    await store.persistAdd(job("a", 1000));
    const exec = new ShardExecutor(0, store, time, limiter, run, () => false, OPTIONS);
    await exec.load();

    time.advance(1000);
    await flush();
    expect(ran).toEqual(["a"]);
    expect(await store.readJobs(0)).toEqual([]);
    await exec.stop();
  });

  it("backs off the poll loop while the silo is overloaded, then runs once clear", async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(2);
    const ran: string[] = [];
    let overloaded = true;
    const run: RunJob = async (j) => {
      ran.push(j.id);
      return completed;
    };
    await store.persistAdd(job("a", 0));
    const exec = new ShardExecutor(0, store, time, limiter, run, () => overloaded, OPTIONS);
    await exec.load();

    // Due immediately, but the silo is overloaded: the poll backs off without running.
    time.advance(0);
    await flush();
    expect(ran).toEqual([]);

    // Clear the overload; the next backed-off poll cycle runs it.
    overloaded = false;
    time.advance(OPTIONS.overloadBackoffMs);
    await flush();
    expect(ran).toEqual(["a"]);
    await exec.stop();
  });

  it("assigns a RunId per claimed job, passes it to the handler, and persists it", async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(2);
    const seen: string[] = [];
    const run: RunJob = async (j) => {
      seen.push(j.runId);
      return completed;
    };
    await store.persistAdd(job("a", 1000));
    const exec = new ShardExecutor(0, store, time, limiter, run, () => false, OPTIONS);
    await exec.load();

    time.advance(1000);
    await flush();

    expect(seen).toHaveLength(1);
    expect(seen[0]).toMatch(/.+/); // a non-empty RunId
    await exec.stop();
  });

  it("skips invocation on re-claim when the persisted RunId is already completed", async () => {
    // Simulate the rebalance hazard: a job ran to completion on silo-A, the
    // completion was persisted (RunId marked completed), but persistRemove was
    // lost (silo crash before remove). Silo-B then claims the shard. The new
    // executor must see the completed RunId and skip invocation — at-most-once
    // for that RunId — rather than double-invoking the handler.
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(2);

    // Pre-seed the store as if silo-A had already run and completed the job,
    // but the final persistRemove was lost.
    const j = job("a", 1000);
    await store.persistAdd(j);
    const priorRunId = "run-from-silo-a";
    await store.persistRunStart(j.shardKey, j.id, priorRunId);
    await store.persistRunComplete(j.shardKey, j.id, priorRunId);

    const calls: string[] = [];
    const run: RunJob = async (ctx) => {
      calls.push(ctx.runId);
      return completed;
    };
    const exec = new ShardExecutor(j.shardKey, store, time, limiter, run, () => false, OPTIONS);
    await exec.load();
    time.advance(1000);
    await flush();

    expect(calls).toEqual([]); // skipped — RunId already marked completed
    expect(await store.readJobs(j.shardKey)).toEqual([]); // and the entry removed
    await exec.stop();
  });

  it("runs a job exactly once when silo-A completes and silo-B re-claims the shard", async () => {
    // Sociable mid-flight rebalance via a shared store: silo-A claims, runs,
    // and successfully marks the RunId completed; before its `persistRemove`
    // lands, the process dies and silo-B claims the shard. silo-B's executor
    // sees the completed-RunId tombstone on load and skips invocation. End
    // result across both silos: the handler ran exactly once.
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(2);
    const calls: string[] = [];

    // Wrap the store on silo-A so its `persistRemove` is lost (the failure mode
    // the dedup defends against), but everything else lands. The runId marker
    // is the durable evidence that the run completed.
    const siloAStore: typeof store = Object.create(store);
    siloAStore.persistRemove = async () => {
      /* dropped — simulates a crash between persistRunComplete and persistRemove */
    };

    await store.persistAdd(job("a", 0));

    const siloA = new ShardExecutor(
      0,
      siloAStore,
      time,
      limiter,
      async (ctx) => {
        calls.push(`A:${ctx.runId}`);
        return completed;
      },
      () => false,
      OPTIONS,
    );
    await siloA.load();
    time.advance(0);
    await flush();
    await siloA.stop();

    // Silo-B comes along, claims the same shard, loads the (still-persisted)
    // job — but the entry carries the completed RunId tombstone from silo-A.
    const siloB = new ShardExecutor(
      0,
      store,
      time,
      limiter,
      async (ctx) => {
        calls.push(`B:${ctx.runId}`);
        return completed;
      },
      () => false,
      OPTIONS,
    );
    await siloB.load();
    time.advance(0);
    await flush();

    expect(calls).toHaveLength(1); // exactly once across both silos
    expect(calls[0]?.startsWith("A:")).toBe(true);
    expect(await store.readJobs(0)).toEqual([]); // silo-B cleaned up the tombstone
    await siloB.stop();
  });

  it("passes the dequeue count to the handler and increments it across re-polls", async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(2);
    const counts: number[] = [];
    const run: RunJob = async (j: JobRunContext) => {
      counts.push(j.dequeueCount);
      return counts.length < 2 ? pollAfter({ seconds: 1 }) : completed;
    };
    await store.persistAdd(job("a", 0));
    const exec = new ShardExecutor(0, store, time, limiter, run, () => false, OPTIONS);
    await exec.load();

    time.advance(0);
    await flush();
    time.advance(1000);
    await flush();
    // pollAfter preserves the dequeue count rather than treating it as a retry.
    expect(counts).toEqual([1, 1]);
    expect(await store.readJobs(0)).toEqual([]);
    await exec.stop();
  });

  it("stop() resolves only after an in-flight run settles (undrained-stop regression)", async () => {
    // Ownership-handoff hazard: if stop() didn't await the in-flight runOne(),
    // a rebalance could let the new shard owner re-run a job whose handler is
    // still executing on the old owner.
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(2);
    let releaseRun: () => void = () => undefined;
    let running = false;
    const run: RunJob = async () => {
      running = true;
      await new Promise<void>((r) => {
        releaseRun = r;
      });
      running = false;
      return completed;
    };
    await store.persistAdd(job("a", 0));
    const exec = new ShardExecutor(0, store, time, limiter, run, () => false, OPTIONS);
    await exec.load();

    time.advance(0);
    await flush();
    expect(running).toBe(true); // the handler is mid-flight

    let stopped = false;
    const stopping = exec.stop().then(() => {
      stopped = true;
    });
    await flush();
    expect(stopped).toBe(false); // stop() must not resolve while the run is in flight

    releaseRun();
    await stopping;
    expect(stopped).toBe(true);
    expect(running).toBe(false); // the handler had fully settled before stop() resolved
  });
});
