// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IDurableJobGrain.cs,
// ISchedulerGrain.cs, IRetryTestGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { DurableJob, JobRunContext } from "@thresh/core/durable-job";
import type { GrainKey } from "@thresh/core/key-kinds";

/** Upstream `UnitTests.GrainInterfaces.IDurableJobGrain`. */
export interface IDurableJobGrain extends GrainKey<string> {
  scheduleJobAsync(
    jobName: string,
    scheduledTime: Date,
    metadata?: Readonly<Record<string, string>>,
  ): Promise<DurableJob>;
  /**
   * Upstream `TryCancelJobAsync` delegates to `ILocalDurableJobManager.TryCancelDurableJobAsync`,
   * which reports whether the cancel raced ahead of execution. This framework's
   * `GrainRuntime.cancelJob` is fire-and-forget (`Promise<void>`), so the grain
   * computes the same "did this prevent the job from running" boolean itself
   * from its own run-tracking bookkeeping (unknown / already-ran job -> false;
   * otherwise cancel and report true) rather than from the framework call.
   */
  tryCancelJobAsync(job: DurableJob): Promise<boolean>;
  hasJobRan(jobId: string): Promise<boolean>;
  waitForJobToRun(jobId: string): Promise<void>;
  getJobExecutionTime(jobId: string): Promise<Date>;
  getJobRun(jobId: string): Promise<JobRunContext>;
}
export const IDurableJobGrain = defineGrainInterface<IDurableJobGrain>(
  "UnitTests.GrainInterfaces.IDurableJobGrain",
);

/** Upstream `UnitTests.GrainInterfaces.ISchedulerGrain`. */
export interface ISchedulerGrain extends GrainKey<string> {
  scheduleJobOnAnotherGrainAsync(
    targetGrainKey: string,
    jobName: string,
    scheduledTime: Date,
  ): Promise<DurableJob>;
}
export const ISchedulerGrain = defineGrainInterface<ISchedulerGrain>(
  "UnitTests.GrainInterfaces.ISchedulerGrain",
);

/** Upstream `UnitTests.GrainInterfaces.IRetryTestGrain`. */
export interface IRetryTestGrain extends GrainKey<string> {
  scheduleJobAsync(
    jobName: string,
    scheduledTime: Date,
    metadata?: Readonly<Record<string, string>>,
  ): Promise<DurableJob>;
  hasJobSucceeded(jobId: string): Promise<boolean>;
  waitForJobToSucceed(jobId: string): Promise<void>;
  getJobExecutionAttemptCount(jobId: string): Promise<number>;
  getJobDequeueCountHistory(jobId: string): Promise<number[]>;
  getFinalJobRun(jobId: string): Promise<JobRunContext>;
}
export const IRetryTestGrain = defineGrainInterface<IRetryTestGrain>(
  "UnitTests.GrainInterfaces.IRetryTestGrain",
);
