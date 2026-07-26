// Ported from dotnet/orleans test/Orleans.Core.Tests/SchedulerTests/OrleansTaskSchedulerAdvancedTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// Same rationale as OrleansTaskSchedulerBasicTests: every method here drives Orleans' internal
// OrleansTaskScheduler/WorkItemGroup turn-execution-order guarantees over a shared .NET thread
// pool. This framework has no analogous per-activation thread scheduler to unit test.
describe("UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests", () => {
  const reason =
    ".NET-specific: exercises Orleans' internal OrleansTaskScheduler/WorkItemGroup turn-execution-order guarantees over a shared thread pool, which has no counterpart in this framework's single-threaded event-loop execution model";

  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_AC_Test",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_AC_WaitTest",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_AC_Turn_Execution_Order",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_Stopped_WorkItemGroup",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_Task_Turn_Execution_Order",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_AC_Current_TaskScheduler",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_AC_ContinueWith_1_Test",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_Task_JoinAll",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_AC_ContinueWith_2_OrleansSched",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests.Sched_Task_SchedulingContext",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
