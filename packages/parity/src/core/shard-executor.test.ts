// Ported from dotnet/orleans test/Orleans.Core.Tests/DurableJobs/ShardExecutorTests.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { completed, failed, pollAfter, type DurableJob } from "@thresh/core/durable-job";
import { GrainId } from "@thresh/core/grain-id";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { systemTimeProvider } from "@thresh/core/time-provider";
import { defaultShouldRetry } from "@thresh/durable-jobs/job-model";
import { MemoryJobShardStore } from "@thresh/durable-jobs/memory-job-shard-store";
import { ConcurrencyLimiter, ShardExecutor, type RunJob } from "@thresh/durable-jobs/shard-executor";

// Upstream's ShardExecutor is invoked via a single `RunShardAsync(shard, cancellationToken)`
// call that processes the shard's *current* jobs and returns once they settle —
// mocking IJobShard, IGrainFactory and IOverloadDetector with NSubstitute. This
// framework's ShardExecutor is a continuously running poll loop over a durable
// `JobShardStore` (`load()` arms it, `stop()` tears it down), driven by an
// injected `TimeProvider` rather than real Task.Delay/CancellationToken. Tests
// below drive the same behaviour (concurrency, overload backoff, slow-start,
// retry/pollAfter outcomes) through that API and a FakeTimeProvider.
const OPTIONS = {
  shardDurationMs: 3_600_000,
  maxConcurrentJobsPerSilo: 10,
  // Equal to maxConcurrentJobsPerSilo: none of these tests exercise slow-start
  // itself (that's RunShardAsync_WithSlowStart_GraduallyIncreasesConcurrency
  // below, which overrides this), so the ceiling should never be the thing
  // that throttles admission here — several tests enqueue multiple
  // simultaneously-due jobs against a FakeTimeProvider and expect them all to
  // run out within a single `advance()` + flush, which a low ceiling (now
  // checked against jobs actually in flight, not just admitted-this-cycle)
  // would stall on since FakeTimeProvider never yields to the completions
  // that would otherwise free it up mid-`advance()`.
  slowStartInitialConcurrency: 10,
  slowStartGrowthFactor: 2,
  // Large enough that none of these tests' brief FakeTimeProvider advances (or
  // short real-clock waits) cross an interval boundary — slow-start growth is
  // exercised specifically by RunShardAsync_WithSlowStart_GraduallyIncreasesConcurrency below.
  slowStartIntervalMs: 1_000_000,
  overloadBackoffMs: 50,
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

describe("NonSilo.Tests.ScheduledJobs.ShardExecutorTests", () => {
  const NS = "NonSilo.Tests.ScheduledJobs.ShardExecutorTests";

  orleansTest(`${NS}.RunShardAsync_WhenNotOverloaded_ProcessesJobsWithoutDelay`, async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(10);
    const completedJobs: string[] = [];
    const run: RunJob = async (j) => {
      completedJobs.push(j.id);
      return completed;
    };
    for (let i = 0; i < 3; i++) await store.persistAdd(job(`job-${i}`, 0));
    const exec = new ShardExecutor(0, store, time, limiter, run, () => false, OPTIONS);
    await exec.load();

    time.advance(0);
    await flush();

    expect(completedJobs).toHaveLength(3);
    expect(completedJobs).toContain("job-0");
    expect(completedJobs).toContain("job-1");
    expect(completedJobs).toContain("job-2");
    expect(await store.readJobs(0)).toEqual([]);
    exec.stop();
  });

  orleansTest(`${NS}.RunShardAsync_WhenOverloaded_PausesAndRetriesWithBackoffDelay`, async () => {
    const store = new MemoryJobShardStore();
    const time = new FakeTimeProvider();
    const limiter = new ConcurrencyLimiter(10);
    const completedJobs: string[] = [];
    let checkCount = 0;
    const isOverloaded = () => {
      checkCount += 1;
      return checkCount <= 3;
    };
    const run: RunJob = async (j) => {
      completedJobs.push(j.id);
      return completed;
    };
    for (let i = 0; i < 2; i++) await store.persistAdd(job(`job-${i}`, 0));
    const exec = new ShardExecutor(0, store, time, limiter, run, isOverloaded, OPTIONS);
    await exec.load();

    for (let i = 0; i < 4; i++) {
      time.advance(OPTIONS.overloadBackoffMs);
      await flush();
    }

    expect(completedJobs).toHaveLength(2);
    expect(checkCount).toBeGreaterThan(3);
    exec.stop();
  });

  orleansTest(
    `${NS}.RunShardAsync_WhenOverloadTransitionsDuringProcessing_HandlesStateChanges`,
    async () => {
      const store = new MemoryJobShardStore();
      const time = new FakeTimeProvider();
      const limiter = new ConcurrencyLimiter(10);
      const completedJobs: string[] = [];
      let checkCount = 0;
      const isOverloaded = () => {
        checkCount += 1;
        return checkCount % 2 === 1;
      };
      const run: RunJob = async (j) => {
        completedJobs.push(j.id);
        return completed;
      };
      for (let i = 0; i < 5; i++) await store.persistAdd(job(`job-${i}`, 0));
      const exec = new ShardExecutor(0, store, time, limiter, run, isOverloaded, OPTIONS);
      await exec.load();

      for (let i = 0; i < 10; i++) {
        time.advance(OPTIONS.overloadBackoffMs);
        await flush();
      }

      expect(completedJobs).toHaveLength(5);
      for (let i = 0; i < 5; i++) expect(completedJobs).toContain(`job-${i}`);
      exec.stop();
    },
  );

  orleansTest(
    `${NS}.RunShardAsync_RespectsMaxConcurrentJobsPerSilo_WhileCheckingOverload`,
    async () => {
      // Uses the real clock (not FakeTimeProvider): the executor's re-poll timer
      // and the job's own completion both need real event-loop turns to
      // interleave here — genuine overlapping concurrency, not just fake-clock
      // due-time bookkeeping.
      const maxConcurrent = 3;
      const store = new MemoryJobShardStore();
      const limiter = new ConcurrencyLimiter(maxConcurrent);
      let current = 0;
      let maxObserved = 0;
      const run: RunJob = async (_j) => {
        current += 1;
        maxObserved = Math.max(maxObserved, current);
        await new Promise((r) => setTimeout(r, 30));
        current -= 1;
        return completed;
      };
      for (let i = 0; i < 10; i++) await store.persistAdd(job(`job-${i}`, 0));
      const exec = new ShardExecutor(0, store, systemTimeProvider, limiter, run, () => false, {
        ...OPTIONS,
        maxConcurrentJobsPerSilo: maxConcurrent,
        slowStartInitialConcurrency: maxConcurrent,
      });
      await exec.load();
      await new Promise((r) => setTimeout(r, 500));

      expect(maxObserved).toBeLessThanOrEqual(maxConcurrent);
      expect(maxObserved).toBeGreaterThan(1);
      exec.stop();
    },
  );

  orleansTest.excluded(
    "TS's ShardExecutor has no single-call RunShardAsync(shard, CancellationToken) that " +
      "returns once a batch settles or the token is canceled — it is a continuously " +
      "running poll loop stopped via `stop()`, and there is no CancellationToken concept " +
      "threaded through the poll/backoff wait to propagate OperationCanceledException.",
    `${NS}.RunShardAsync_WhenCancelledDuringOverloadBackoff_CancelsCleanly`,
  );

  orleansTest(
    `${NS}.RunShardAsync_WhenJobFailsDuringOverload_ContinuesOverloadChecking`,
    async () => {
      const store = new MemoryJobShardStore();
      const time = new FakeTimeProvider();
      const limiter = new ConcurrencyLimiter(10);
      let checkCount = 0;
      const isOverloaded = () => {
        checkCount += 1;
        return checkCount % 3 === 1;
      };
      const completedJobs: string[] = [];
      const failedJobs: string[] = [];
      let execCount = 0;
      const run: RunJob = async (j) => {
        execCount += 1;
        if (execCount === 1) {
          failedJobs.push(j.id);
          return failed(new Error("boom"));
        }
        completedJobs.push(j.id);
        return completed;
      };
      for (let i = 0; i < 3; i++) await store.persistAdd(job(`job-${i}`, 0));
      const exec = new ShardExecutor(0, store, time, limiter, run, isOverloaded, {
        ...OPTIONS,
        shouldRetry: () => ({ seconds: 1 }),
      });
      await exec.load();

      // Advance only far enough for the overload checks and the two successful
      // jobs to settle — well short of the 1s retry backoff, so the retried job
      // has not yet re-fired (this test is about overload checking continuing
      // through a failure, not about the eventual retry outcome).
      for (let i = 0; i < 5; i++) {
        time.advance(OPTIONS.overloadBackoffMs);
        await flush();
      }

      expect(completedJobs).toHaveLength(2);
      expect(failedJobs).toHaveLength(1);
      exec.stop();
    },
  );

  orleansTest.excluded(
    "There is no shard-level 'start time' distinct from individual job due times " +
      "(`shardActivationBufferMs` is a resolved option field but is not consulted by " +
      "ShardExecutor — see local-durable-job-manager.ts/shard-executor.ts). Jobs each " +
      "carry their own due time; the shard itself has no StartTime gate to wait on " +
      "before processing.",
    `${NS}.RunShardAsync_WaitsForShardStartTime_BeforeProcessing`,
  );

  orleansTest.excluded(
    "There is no single RunShardAsync call whose return signals 'this batch of jobs " +
      "has fully settled' — the executor is a continuously running poll loop with no " +
      "batch-completion await to assert against.",
    `${NS}.RunShardAsync_WaitsForAllJobsToComplete_BeforeReturning`,
  );

  orleansTest(
    `${NS}.RunShardAsync_WhenJobReturnsPollAfter_EntersPollingLoopUntilCompletion`,
    async () => {
      const store = new MemoryJobShardStore();
      const time = new FakeTimeProvider();
      const limiter = new ConcurrencyLimiter(10);
      let calls = 0;
      const run: RunJob = async () => {
        calls += 1;
        return calls < 4 ? pollAfter({ ms: 10 }) : completed;
      };
      await store.persistAdd(job("job-0", 0));
      const exec = new ShardExecutor(0, store, time, limiter, run, () => false, OPTIONS);
      await exec.load();

      time.advance(0);
      await flush();
      time.advance(10);
      await flush();
      time.advance(10);
      await flush();
      time.advance(10);
      await flush();

      expect(calls).toBe(4);
      expect(await store.readJobs(0)).toEqual([]);
      exec.stop();
    },
  );

  orleansTest(
    `${NS}.RunShardAsync_WhenJobReturnsPollAfterThenFails_HandlesFailureCorrectly`,
    async () => {
      const store = new MemoryJobShardStore();
      const time = new FakeTimeProvider();
      const limiter = new ConcurrencyLimiter(10);
      let calls = 0;
      const run: RunJob = async () => {
        calls += 1;
        if (calls < 4) return pollAfter({ ms: 10 });
        return failed(new Error("boom"));
      };
      await store.persistAdd(job("job-0", 0));
      const exec = new ShardExecutor(0, store, time, limiter, run, () => false, {
        ...OPTIONS,
        shouldRetry: () => ({ seconds: 1 }),
      });
      await exec.load();

      time.advance(0);
      await flush();
      time.advance(10);
      await flush();
      time.advance(10);
      await flush();
      time.advance(10);
      await flush();

      expect(calls).toBe(4);
      // Retried (not removed): the persisted job is still there, due later.
      const persisted = await store.readJobs(0);
      expect(persisted).toHaveLength(1);
      expect(persisted[0]?.job.id).toBe("job-0");
      exec.stop();
    },
  );

  orleansTest(
    `${NS}.RunShardAsync_WhenRetryPersistenceFails_ReleasesConcurrencyAndContinuesProcessing`,
    async () => {
      const store = new MemoryJobShardStore();
      // Wrap the store so persistRetry always throws (the failure this test
      // simulates); TS's reschedule() fires it via a fire-and-forget `.catch`,
      // so a persistence failure here never blocks subsequent processing.
      const failingStore: typeof store = Object.create(store);
      failingStore.persistRetry = async () => {
        throw new Error("simulated retry persistence failure");
      };
      // Real clock: two jobs contend for one concurrency slot, so the second
      // only runs once the first settles and releases it — needs a genuine
      // event-loop turn between them, not FakeTimeProvider's synchronous advance.
      const limiter = new ConcurrencyLimiter(1);
      const completedJobs: string[] = [];
      const failedJobs: string[] = [];
      let execCount = 0;
      const run: RunJob = async (j) => {
        execCount += 1;
        if (execCount === 1) {
          failedJobs.push(j.id);
          return failed(new Error("boom"));
        }
        completedJobs.push(j.id);
        return completed;
      };
      for (let i = 0; i < 2; i++) await store.persistAdd(job(`job-${i}`, 0));
      const exec = new ShardExecutor(
        0,
        failingStore,
        systemTimeProvider,
        limiter,
        run,
        () => false,
        {
          ...OPTIONS,
          shouldRetry: () => ({ seconds: 1 }),
        },
      );
      await exec.load();

      await new Promise((r) => setTimeout(r, 200));

      expect(failedJobs).toHaveLength(1);
      expect(completedJobs).toHaveLength(1);
      exec.stop();
    },
  );

  orleansTest.excluded(
    "There is no CancellationToken concept for an individual job's execution; a " +
      "canceled run is indistinguishable here from any other failure — `runJob` throwing " +
      "(for any reason) is caught and turned into a `failed` result, subject to the " +
      "ordinary retry policy. There is no special 'canceled: skip retry and skip remove' " +
      "code path to test.",
    `${NS}.RunShardAsync_WhenExecutionIsCanceled_DoesNotRetryOrRemove`,
  );

  // Orleans' slow-start ramps the concurrency ceiling on a real-time interval
  // (`slowStartInterval`, e.g. every 100ms). This framework's ShardExecutor
  // options carry the equivalent `slowStartIntervalMs`, and the ceiling only
  // grows once that much wall-clock time (per the injected TimeProvider) has
  // elapsed since the shard started — a large backlog cannot ramp straight to
  // max concurrency within a handful of same-tick poll cycles.
  orleansTest(`${NS}.RunShardAsync_WithSlowStart_GraduallyIncreasesConcurrency`, async () => {
    const store = new MemoryJobShardStore();
    const limiter = new ConcurrencyLimiter(16);
    let current = 0;
    let maxObserved = 0;
    const run: RunJob = async () => {
      current += 1;
      maxObserved = Math.max(maxObserved, current);
      await new Promise((r) => setTimeout(r, 20));
      current -= 1;
      return completed;
    };
    for (let i = 0; i < 20; i++) await store.persistAdd(job(`job-${i}`, 0));
    const exec = new ShardExecutor(0, store, systemTimeProvider, limiter, run, () => false, {
      ...OPTIONS,
      maxConcurrentJobsPerSilo: 16,
      slowStartInitialConcurrency: 2,
      slowStartGrowthFactor: 2,
      slowStartIntervalMs: 100,
    });
    await exec.load();

    // Well before the first job's own 20ms run completes (and far short of the
    // 100ms slowStartIntervalMs), only the slow-start initial concurrency (2)
    // has run — a backlog of same-instant due jobs re-polls on a tight loop,
    // but must not ramp the ceiling past its initial value in that time.
    await new Promise((r) => setTimeout(r, 15));
    expect(maxObserved).toBeLessThanOrEqual(2);

    await new Promise((r) => setTimeout(r, 500));
    exec.stop();

    // Across the whole run the ceiling ramped above the initial concurrency.
    expect(maxObserved).toBeGreaterThan(2);
  });

  orleansTest(
    `${NS}.RunShardAsync_WithSlowStartDisabled_UsesFullConcurrencyImmediately`,
    async () => {
      // "Slow-start disabled" has no separate boolean flag here; the equivalent
      // configuration is starting the ceiling at the full per-silo max. Real
      // clock for the same reason as the test above.
      const maxConcurrency = 16;
      const store = new MemoryJobShardStore();
      const limiter = new ConcurrencyLimiter(maxConcurrency);
      let current = 0;
      let maxObserved = 0;
      const run: RunJob = async () => {
        current += 1;
        maxObserved = Math.max(maxObserved, current);
        await new Promise((r) => setTimeout(r, 20));
        current -= 1;
        return completed;
      };
      for (let i = 0; i < 20; i++) await store.persistAdd(job(`job-${i}`, 0));
      const exec = new ShardExecutor(0, store, systemTimeProvider, limiter, run, () => false, {
        ...OPTIONS,
        maxConcurrentJobsPerSilo: maxConcurrency,
        slowStartInitialConcurrency: maxConcurrency,
      });
      await exec.load();

      await new Promise((r) => setTimeout(r, 100));

      expect(maxObserved).toBeGreaterThan(2);
      exec.stop();
    },
  );

  orleansTest.excluded(
    "No options validator exists for durable-jobs/ShardExecutorOptions in this " +
      "framework — options are consumed as-is by `resolveOptions`/`ShardExecutor`, with " +
      "no throw-on-invalid-config validation pass to exercise (see " +
      "ShardClaimBudgetTests' ValidateConfiguration_* exclusions for the same gap).",
    `${NS}.ValidateConfiguration_WithSlowStartDisabled_AllowsNonPositiveInitialConcurrency`,
  );
});
