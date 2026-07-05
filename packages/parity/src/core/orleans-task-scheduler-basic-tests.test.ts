// Ported from dotnet/orleans test/Orleans.Core.Tests/SchedulerTests/OrleansTaskSchedulerBasicTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// Every method in this file exercises Orleans' internal OrleansTaskScheduler / WorkItemGroup /
// ActivationTaskScheduler thread-pool scheduling machinery directly (turn ordering across a
// shared .NET thread pool, TaskScheduler.Current propagation, closure work items). This
// framework has no per-grain thread scheduler: single grain calls are serialized on Node's
// single-threaded event loop instead. Per the assignment's excluded-category list
// ("thread schedulers"), the whole file is excluded.
describe("UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests", () => {
  const reason =
    ".NET-specific: exercises Orleans' internal OrleansTaskScheduler/ActivationTaskScheduler thread-pool turn scheduling, which has no counterpart in this framework's single-threaded event-loop execution model";

  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests.Async_Task_Start_ActivationTaskScheduler",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests.Sched_SimpleFifoTest",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests.Sched_Task_TplFifoTest",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests.Sched_Task_ClosureWorkItem_Wait",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests.Sched_Task_TaskWorkItem_CurrentScheduler",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests.Sched_Task_SubTaskExecutionSequencing",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests.Sched_AC_RequestContext_StartNew_ContinueWith",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerBasicTests.RequestContextProtectedInQueuedTasksTest",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
