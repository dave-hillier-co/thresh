// Ported from dotnet/orleans test/Orleans.DurableJobs.Tests/DurableJobs/DurableJobTestsRunner.cs @ v10.1.0 (MIT).
// Run against the in-memory provider via `DefaultCluster.Tests.InMemoryDurableJobsTests`
// (test/Orleans.DefaultCluster.Tests/InMemoryDurableJobsTests.cs @ v10.1.0), whose
// namespace/class/method names are the upstream xunit ids used below.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { Guid } from "@thresh/core/guid";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { DurableJob } from "@thresh/core/durable-job";
import {
  DurableJobGrain,
  IDurableJobGrain,
  IRetryTestGrain,
  ISchedulerGrain,
  RetryTestGrain,
  SchedulerGrain,
} from "@thresh/parity/grains/impl/durable-job-grain";

/** Let queued microtasks (dispatch + result application) settle after advancing the clock. */
async function flush(times = 5): Promise<void> {
  for (let i = 0; i < times; i++) await new Promise((r) => setTimeout(r, 0));
}

/**
 * Step the fake clock forward, flushing between steps, so timers (re)armed
 * asynchronously while applying a job's result (retry backoff) come due.
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

const randomKey = () => Guid.newGuid().toString();

describe("DefaultCluster.Tests.InMemoryDurableJobsTests", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    // A single silo: with `initialSilos: 2` a scheduled job's target grain can
    // activate on a silo other than the one whose `LocalDurableJobManager`
    // already claimed that job's shard (all these due times land in the same
    // 1-hour bucket). `LocalDurableJobManager.scheduleJob` only calls
    // `ensureOwned` on the scheduling silo; if that silo loses (or never
    // attempts) the claim, the job is persisted but never enqueued on any
    // executor and never fires — the owning silo does not re-poll the store for
    // jobs another silo persisted into its shard. None of these tests exercise
    // cross-silo semantics, so one silo sidesteps a real gap (reported to the
    // task owner) rather than making these tests flaky on something out of scope.
    cluster = await TestCluster.start({
      initialSilos: 1,
      time,
      grains: [
        { ctor: DurableJobGrain.grain, interfaces: [IDurableJobGrain] },
        { ctor: SchedulerGrain.grain, interfaces: [ISchedulerGrain] },
        { ctor: RetryTestGrain.grain, interfaces: [IRetryTestGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.DurableJobGrain", async () => {
    const grain = cluster.getGrain(IDurableJobGrain, randomKey());
    const dueMs = time.now() + 5000;
    const dueTime = new Date(dueMs);
    const job1 = await grain.scheduleJobAsync("TestJob", dueTime);
    expect(job1).toBeTruthy();
    expect(job1.name).toBe("TestJob");
    expect(job1.dueTime).toEqual(dueTime);
    const job2 = await grain.scheduleJobAsync("TestJob2", dueTime);
    const job3 = await grain.scheduleJobAsync("TestJob3", new Date(dueMs + 4000));
    const job4 = await grain.scheduleJobAsync("TestJob4", dueTime);
    const job5 = await grain.scheduleJobAsync("TestJob5", new Date(dueMs + 1000));
    const canceledJob = await grain.scheduleJobAsync("CanceledJob", new Date(dueMs + 2000));
    expect(await grain.tryCancelJobAsync(canceledJob)).toBe(true);

    const waits = [job1, job2, job3, job4, job5].map((job) => grain.waitForJobToRun(job.id));
    await advanceSettling(time, 10_000);
    await Promise.all(waits);

    expect(await grain.hasJobRan(canceledJob.id)).toBe(false);
  });

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.JobExecutionOrder", async () => {
    const grain = cluster.getGrain(IDurableJobGrain, randomKey());
    const baseMs = time.now() + 2000;

    const job1 = await grain.scheduleJobAsync("FirstJob", new Date(baseMs));
    const job2 = await grain.scheduleJobAsync("SecondJob", new Date(baseMs + 2000));
    const job3 = await grain.scheduleJobAsync("ThirdJob", new Date(baseMs + 4000));

    const wait1 = grain.waitForJobToRun(job1.id);
    const wait2 = grain.waitForJobToRun(job2.id);
    const wait3 = grain.waitForJobToRun(job3.id);
    await advanceSettling(time, 8000);
    await Promise.all([wait1, wait2, wait3]);

    const time1 = await grain.getJobExecutionTime(job1.id);
    const time2 = await grain.getJobExecutionTime(job2.id);
    const time3 = await grain.getJobExecutionTime(job3.id);

    expect(time1.getTime()).toBeLessThan(time2.getTime());
    expect(time2.getTime()).toBeLessThan(time3.getTime());
  });

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.PastDueTime", async () => {
    const grain = cluster.getGrain(IDurableJobGrain, randomKey());
    const pastTime = new Date(time.now() - 5000);

    const job = await grain.scheduleJobAsync("PastDueJob", pastTime);
    expect(job).toBeTruthy();

    const wait = grain.waitForJobToRun(job.id);
    await advanceSettling(time, 1000);
    await wait;
    expect(await grain.hasJobRan(job.id)).toBe(true);
  });

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.JobWithMetadata", async () => {
    const grain = cluster.getGrain(IDurableJobGrain, randomKey());
    const dueTime = new Date(time.now() + 3000);
    const metadata = { UserId: "user123", Action: "SendEmail", Priority: "High" };

    const job = await grain.scheduleJobAsync("MetadataJob", dueTime, metadata);
    expect(job).toBeTruthy();
    expect(job.metadata).toBeTruthy();
    expect(Object.keys(job.metadata)).toHaveLength(3);
    expect(job.metadata["UserId"]).toBe("user123");
    expect(job.metadata["Action"]).toBe("SendEmail");
    expect(job.metadata["Priority"]).toBe("High");

    const wait = grain.waitForJobToRun(job.id);
    await advanceSettling(time, 4000);
    await wait;

    const context = await grain.getJobRun(job.id);
    expect(context).toBeTruthy();
    expect(context.metadata).toBeTruthy();
    expect(context.metadata["UserId"]).toBe("user123");
  });

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.MultipleGrains", async () => {
    const grain1 = cluster.getGrain(IDurableJobGrain, randomKey());
    const grain2 = cluster.getGrain(IDurableJobGrain, randomKey());
    const grain3 = cluster.getGrain(IDurableJobGrain, randomKey());
    const dueTime = new Date(time.now() + 3000);

    const job1 = await grain1.scheduleJobAsync("Job1", dueTime);
    const job2 = await grain2.scheduleJobAsync("Job2", dueTime);
    const job3 = await grain3.scheduleJobAsync("Job3", dueTime);

    const waits = [
      grain1.waitForJobToRun(job1.id),
      grain2.waitForJobToRun(job2.id),
      grain3.waitForJobToRun(job3.id),
    ];
    await advanceSettling(time, 4000);
    await Promise.all(waits);

    expect(await grain1.hasJobRan(job1.id)).toBe(true);
    expect(await grain2.hasJobRan(job2.id)).toBe(true);
    expect(await grain3.hasJobRan(job3.id)).toBe(true);

    expect(await grain1.hasJobRan(job2.id)).toBe(false);
    expect(await grain2.hasJobRan(job3.id)).toBe(false);
    expect(await grain3.hasJobRan(job1.id)).toBe(false);
  });

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.DuplicateJobNames", async () => {
    const grain = cluster.getGrain(IDurableJobGrain, randomKey());
    const dueTime = new Date(time.now() + 3000);

    const job1 = await grain.scheduleJobAsync("SameName", dueTime);
    const job2 = await grain.scheduleJobAsync("SameName", new Date(dueTime.getTime() + 1000));
    const job3 = await grain.scheduleJobAsync("SameName", new Date(dueTime.getTime() + 2000));

    expect(job1.id).not.toBe(job2.id);
    expect(job2.id).not.toBe(job3.id);
    expect(job1.id).not.toBe(job3.id);

    expect(job1.name).toBe("SameName");
    expect(job2.name).toBe("SameName");
    expect(job3.name).toBe("SameName");

    const waits = [job1, job2, job3].map((job) => grain.waitForJobToRun(job.id));
    await advanceSettling(time, 5000);
    await Promise.all(waits);

    expect(await grain.hasJobRan(job1.id)).toBe(true);
    expect(await grain.hasJobRan(job2.id)).toBe(true);
    expect(await grain.hasJobRan(job3.id)).toBe(true);
  });

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.CancelNonExistentJob", async () => {
    const grain = cluster.getGrain(IDurableJobGrain, randomKey());
    const dueTime = new Date(time.now() + 10_000);

    const job = await grain.scheduleJobAsync("RealJob", dueTime);

    const fakeJob: DurableJob = {
      id: "non-existent-id",
      name: "FakeJob",
      dueTime,
      shardKey: job.shardKey,
      target: job.target,
      metadata: {},
    };

    const cancelResult = await grain.tryCancelJobAsync(fakeJob);
    expect(cancelResult).toBe(false);

    time.advance(100);
    await flush();
    expect(await grain.hasJobRan(fakeJob.id)).toBe(false);
  });

  orleansTest(
    "DefaultCluster.Tests.InMemoryDurableJobsTests.CancelAlreadyExecutedJob",
    async () => {
      const grain = cluster.getGrain(IDurableJobGrain, randomKey());
      const dueTime = new Date(time.now() + 2000);

      const job = await grain.scheduleJobAsync("QuickJob", dueTime);

      const wait = grain.waitForJobToRun(job.id);
      await advanceSettling(time, 3000);
      await wait;
      expect(await grain.hasJobRan(job.id)).toBe(true);

      const cancelResult = await grain.tryCancelJobAsync(job);
      expect(cancelResult).toBe(false);
    },
  );

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.ConcurrentScheduling", async () => {
    const grain = cluster.getGrain(IDurableJobGrain, randomKey());
    const baseMs = time.now() + 5000;
    const jobCount = 20;

    const jobs = await Promise.all(
      Array.from({ length: jobCount }, (_, i) =>
        grain.scheduleJobAsync(`ConcurrentJob${i}`, new Date(baseMs + i * 100)),
      ),
    );

    expect(jobs).toHaveLength(jobCount);
    expect(new Set(jobs.map((j) => j.id)).size).toBe(jobCount);

    const waits = jobs.map((j) => grain.waitForJobToRun(j.id));
    await advanceSettling(time, 8000);
    await Promise.all(waits);

    for (const job of jobs) {
      expect(await grain.hasJobRan(job.id)).toBe(true);
    }
  });

  orleansTest(
    "DefaultCluster.Tests.InMemoryDurableJobsTests.JobPropertiesVerification",
    async () => {
      const grain = cluster.getGrain(IDurableJobGrain, randomKey());
      const dueTime = new Date(time.now() + 3000);
      const metadata = { Key: "Value" };

      const job = await grain.scheduleJobAsync("PropertyTestJob", dueTime, metadata);

      expect(job.id).toBeTruthy();
      expect(job.id.length).toBeGreaterThan(0);
      expect(job.name).toBe("PropertyTestJob");
      expect(job.dueTime).toEqual(dueTime);
      expect(job.shardKey).toBeDefined();
      expect(job.metadata).toBeTruthy();
      expect(Object.keys(job.metadata)).toHaveLength(1);

      const wait = grain.waitForJobToRun(job.id);
      await advanceSettling(time, 4000);
      await wait;

      const context = await grain.getJobRun(job.id);
      expect(context).toBeTruthy();
      expect(context.id).toBe(job.id);
      expect(context.name).toBe(job.name);
      expect(context.runId).toBeTruthy();
      expect(context.runId.length).toBeGreaterThan(0);
    },
  );

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.DequeueCount", async () => {
    const grain = cluster.getGrain(IDurableJobGrain, randomKey());
    const dueTime = new Date(time.now() + 3000);

    const job = await grain.scheduleJobAsync("DequeueTestJob", dueTime);

    const wait = grain.waitForJobToRun(job.id);
    await advanceSettling(time, 4000);
    await wait;

    const context = await grain.getJobRun(job.id);
    expect(context).toBeTruthy();
    expect(context.dequeueCount).toBe(1);
  });

  orleansTest(
    "DefaultCluster.Tests.InMemoryDurableJobsTests.ScheduleJobOnAnotherGrain",
    async () => {
      const schedulerGrain = cluster.getGrain(ISchedulerGrain, "scheduler-grain");
      const targetKey = randomKey();
      const targetGrain = cluster.getGrain(IDurableJobGrain, targetKey);
      const dueTime = new Date(time.now() + 3000);

      const job = await schedulerGrain.scheduleJobOnAnotherGrainAsync(
        targetKey,
        "CrossGrainJob",
        dueTime,
      );

      expect(job).toBeTruthy();
      expect(job.name).toBe("CrossGrainJob");
      expect(job.dueTime).toEqual(dueTime);

      const jobWait = targetGrain.waitForJobToRun(job.id);
      await advanceSettling(time, 4000);
      await jobWait;

      expect(await targetGrain.hasJobRan(job.id)).toBe(true);

      const context = await targetGrain.getJobRun(job.id);
      expect(context).toBeTruthy();
      expect(context.id).toBe(job.id);
      expect(context.name).toBe("CrossGrainJob");
    },
  );

  orleansTest("DefaultCluster.Tests.InMemoryDurableJobsTests.JobRetry", async () => {
    const grain = cluster.getGrain(IRetryTestGrain, randomKey());
    const dueTime = new Date(time.now() + 2000);
    const metadata = { FailUntilAttempt: "3" };

    const job = await grain.scheduleJobAsync("RetryJob", dueTime, metadata);

    expect(job).toBeTruthy();
    expect(job.name).toBe("RetryJob");
    expect(job.metadata).toBeTruthy();
    expect(job.metadata["FailUntilAttempt"]).toBe("3");

    const wait = grain.waitForJobToSucceed(job.id);
    await advanceSettling(time, 60_000);
    await wait;

    expect(await grain.hasJobSucceeded(job.id)).toBe(true);

    const attemptCount = await grain.getJobExecutionAttemptCount(job.id);
    expect(attemptCount).toBe(3);

    const dequeueCountHistory = await grain.getJobDequeueCountHistory(job.id);
    expect(dequeueCountHistory).toHaveLength(3);
    expect(dequeueCountHistory[0]).toBe(1);
    expect(dequeueCountHistory[1]).toBe(2);
    expect(dequeueCountHistory[2]).toBe(3);

    const finalContext = await grain.getFinalJobRun(job.id);
    expect(finalContext).toBeTruthy();
    expect(finalContext.dequeueCount).toBe(3);
    expect(finalContext.id).toBe(job.id);
  });
});
