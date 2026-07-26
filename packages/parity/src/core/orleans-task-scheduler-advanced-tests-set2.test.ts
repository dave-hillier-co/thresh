// Ported from dotnet/orleans test/Orleans.Core.Tests/SchedulerTests/OrleansTaskSchedulerAdvancedTests_Set2.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// Same rationale as OrleansTaskSchedulerBasicTests/AdvancedTests: every method here drives
// Orleans' internal ActivationTaskScheduler (Task.Run/Task.Delay/WhenAny semantics re-hosted on
// the per-activation scheduler), which has no counterpart in this framework's single-threaded
// event-loop execution model.
describe("UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2", () => {
  const reason =
    ".NET-specific: exercises Orleans' internal ActivationTaskScheduler re-hosting of Task.Run/Task.Delay/Task.WhenAny, which has no counterpart in this framework's single-threaded event-loop execution model";

  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_SimpleFifoTest",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_NewTask_ContinueWith_Wrapped",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_SubTaskExecutionSequencing",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_ContinueWith_1_Test",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_WhenAny",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_WhenAny_Timeout",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_WhenAny_Busy_Timeout",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_Task_Run",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_Task_Run_Delay",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_Task_Delay",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_Turn_Execution_Order_Loop",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_Test1",
  );
  orleansTest.excluded(
    reason,
    "UnitTests.SchedulerTests.OrleansTaskSchedulerAdvancedTests_Set2.ActivationSched_Test1_Bounce",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
