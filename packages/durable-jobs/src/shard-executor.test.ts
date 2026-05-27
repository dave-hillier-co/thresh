import { describe, expect, it } from "vitest";
import { completed, pollAfter, type DurableJob, type JobRunContext } from "@tsva/core/durable-job";
import { GrainId } from "@tsva/core/grain-id";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { defaultShouldRetry } from "@tsva/durable-jobs/job-model";
import { MemoryJobShardStore } from "@tsva/durable-jobs/memory-job-shard-store";
import { ConcurrencyLimiter, ShardExecutor, type RunJob } from "@tsva/durable-jobs/shard-executor";

const OPTIONS = {
  shardDurationMs: 3_600_000,
  maxConcurrentJobsPerSilo: 2,
  slowStartInitialConcurrency: 1,
  slowStartGrowthFactor: 2,
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
    exec.stop();
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
    exec.stop();
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
    exec.stop();
  });
});
