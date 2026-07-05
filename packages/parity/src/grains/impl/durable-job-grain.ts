// Ported from dotnet/orleans test/Grains/TestGrains/DurableJobGrain.cs,
// SchedulerGrain.cs, RetryTestGrain.cs @ v10.1.0 (MIT).
import { defineGrain, useDurableJobHandler } from "@tsva/core/define-grain";
import { completed, type DurableJob, type JobRunContext } from "@tsva/core/durable-job";
import { GrainId } from "@tsva/core/grain-id";
import {
  IDurableJobGrain,
  IRetryTestGrain,
  ISchedulerGrain,
} from "@tsva/parity/grains/interfaces/durable-job-grain-interfaces";

export { IDurableJobGrain, IRetryTestGrain, ISchedulerGrain };

/** A `TaskCompletionSource<void>` stand-in: a promise plus its own resolver. */
interface Deferred<T> {
  promise: Promise<T>;
  resolve: (value: T) => void;
}
function defer<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((r) => (resolve = r));
  return { promise, resolve };
}

export const DURABLE_JOB_GRAIN_TYPE = "UnitTests.Grains.DurableJobGrain";

// A durable job's handler runs as a turn (single-threaded per activation), so
// successive runs are strictly ordered; a monotonic tie-breaker keeps recorded
// execution `Date`s strictly increasing even when two turns land in the same
// millisecond of wall-clock time (upstream's `JobExecutionOrder` test asserts
// `time1 < time2 < time3`).
let executionSequence = 0;
function monotonicNow(): Date {
  executionSequence += 1;
  return new Date(Date.now() + executionSequence);
}

/**
 * Upstream `UnitTests.Grains.DurableJobGrain`: schedules jobs on itself and
 * records each run so tests can observe execution order, timing and metadata.
 * `[AlwaysInterleave]` on `WaitForJobToRun` (a turn that blocks on a promise
 * resolved by another turn on the same activation, the durable-job handler) is
 * ported as `reentrant: true` on the whole grain.
 */
export const DurableJobGrain = defineGrain<IDurableJobGrain>(
  DURABLE_JOB_GRAIN_TYPE,
  (ctx) => {
    const waiters = new Map<string, Deferred<void>>();
    const hasRun = new Set<string>();
    const executionTimes = new Map<string, Date>();
    const runContexts = new Map<string, JobRunContext>();

    function waiterFor(jobId: string): Deferred<void> {
      let waiter = waiters.get(jobId);
      if (waiter === undefined) {
        waiter = defer<void>();
        waiters.set(jobId, waiter);
      }
      return waiter;
    }

    useDurableJobHandler(ctx, async (job) => {
      executionTimes.set(job.id, monotonicNow());
      runContexts.set(job.id, job);
      hasRun.add(job.id);
      waiterFor(job.id).resolve();
      return completed;
    });

    return {
      scheduleJobAsync: async (jobName, scheduledTime, metadata) => {
        const job = await ctx.runtime.scheduleJob({
          target: ctx.id,
          name: jobName,
          dueTime: scheduledTime,
          ...(metadata !== undefined ? { metadata } : {}),
        });
        waiterFor(job.id); // pre-arm so a WaitForJobToRun race always finds a waiter
        return job;
      },
      tryCancelJobAsync: async (job: DurableJob) => {
        if (hasRun.has(job.id) || !waiters.has(job.id)) return false;
        await ctx.runtime.cancelJob(job);
        return true;
      },
      hasJobRan: async (jobId) => hasRun.has(jobId),
      waitForJobToRun: async (jobId) => {
        await waiterFor(jobId).promise;
      },
      getJobExecutionTime: async (jobId) => {
        const time = executionTimes.get(jobId);
        if (time === undefined) {
          throw new Error(`Job ${jobId} has not executed or was not scheduled on this grain.`);
        }
        return time;
      },
      getJobRun: async (jobId) => {
        const context = runContexts.get(jobId);
        if (context === undefined) {
          throw new Error(`Job ${jobId} has not executed or was not scheduled on this grain.`);
        }
        return context;
      },
    };
  },
  { reentrant: true },
);

/** Upstream `UnitTests.Grains.SchedulerGrain`: schedules a job targeting another grain. */
export const SchedulerGrain = defineGrain<ISchedulerGrain>(
  "UnitTests.Grains.SchedulerGrain",
  (ctx) => ({
    scheduleJobOnAnotherGrainAsync: async (targetGrainKey, jobName, scheduledTime) =>
      ctx.runtime.scheduleJob({
        target: new GrainId(DURABLE_JOB_GRAIN_TYPE, targetGrainKey),
        name: jobName,
        dueTime: scheduledTime,
      }),
  }),
);

/**
 * Upstream `UnitTests.Grains.RetryTestGrain`: fails on every attempt whose
 * `DequeueCount` is below the job's `FailUntilAttempt` metadata, then succeeds.
 */
export const RetryTestGrain = defineGrain<IRetryTestGrain>(
  "UnitTests.Grains.RetryTestGrain",
  (ctx) => {
    const succeeded = new Map<string, Deferred<void>>();
    const done = new Set<string>();
    const attempts = new Map<string, number>();
    const dequeueCountHistory = new Map<string, number[]>();
    const finalContexts = new Map<string, JobRunContext>();

    function waiterFor(jobId: string): Deferred<void> {
      let waiter = succeeded.get(jobId);
      if (waiter === undefined) {
        waiter = defer<void>();
        succeeded.set(jobId, waiter);
      }
      return waiter;
    }

    useDurableJobHandler(ctx, async (job) => {
      const attempt = (attempts.get(job.id) ?? 0) + 1;
      attempts.set(job.id, attempt);
      const history = dequeueCountHistory.get(job.id) ?? [];
      history.push(job.dequeueCount);
      dequeueCountHistory.set(job.id, history);

      const failUntilAttempt = job.metadata["FailUntilAttempt"];
      if (typeof failUntilAttempt === "string") {
        const threshold = Number(failUntilAttempt);
        if (!Number.isNaN(threshold) && job.dequeueCount < threshold) {
          throw new Error(`Simulated failure for job ${job.id} on attempt ${attempt}`);
        }
      }

      finalContexts.set(job.id, job);
      done.add(job.id);
      waiterFor(job.id).resolve();
      return completed;
    });

    return {
      scheduleJobAsync: async (jobName, scheduledTime, metadata) => {
        const job = await ctx.runtime.scheduleJob({
          target: ctx.id,
          name: jobName,
          dueTime: scheduledTime,
          ...(metadata !== undefined ? { metadata } : {}),
        });
        waiterFor(job.id);
        return job;
      },
      hasJobSucceeded: async (jobId) => done.has(jobId),
      waitForJobToSucceed: async (jobId) => {
        await waiterFor(jobId).promise;
      },
      getJobExecutionAttemptCount: async (jobId) => {
        const count = attempts.get(jobId);
        if (count === undefined) {
          throw new Error(
            `Job ${jobId} has not been attempted or was not scheduled on this grain.`,
          );
        }
        return count;
      },
      getJobDequeueCountHistory: async (jobId) => {
        const history = dequeueCountHistory.get(jobId);
        if (history === undefined) {
          throw new Error(
            `Job ${jobId} has not been attempted or was not scheduled on this grain.`,
          );
        }
        return history;
      },
      getFinalJobRun: async (jobId) => {
        const context = finalContexts.get(jobId);
        if (context === undefined) {
          throw new Error(`Job ${jobId} has not succeeded or was not scheduled on this grain.`);
        }
        return context;
      },
    };
  },
  { reentrant: true },
);
