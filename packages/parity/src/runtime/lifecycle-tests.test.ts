// Ported from dotnet/orleans test/Orleans.Runtime.Tests/Lifecycle/LifecycleTests.cs @ v10.1.0 (MIT).
// This exercises Orleans.Runtime's LifecycleSubject: a numbered-stage
// ILifecycleObservable/ILifecycleObserver protocol (Subscribe(stage, observer),
// OnStart()/OnStop() driving observers stage-by-stage, with cascading
// stage-abort semantics on a failed OnStart). This framework has no equivalent
// multi-stage lifecycle-subject primitive; grain/silo lifecycle hooks here are
// a flat before/after activation pair (see GrainLifecycle in
// packages/core/src/define-grain.ts), not a staged pub/sub subject usable
// standalone as in these tests.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.LifecycleTests", () => {
  const testClass = "Tester.LifecycleTests";

  orleansTest.gap("GAP-LIFECYCLE-SUBJECT", `${testClass}.FullLifecycleTest`);
  orleansTest.gap("GAP-LIFECYCLE-SUBJECT", `${testClass}.FailOnStartOnEachStageLifecycleTest`);
  orleansTest.gap("GAP-LIFECYCLE-SUBJECT", `${testClass}.FailOnStopOnEachStageLifecycleTest`);
  orleansTest.gap("GAP-LIFECYCLE-SUBJECT", `${testClass}.MultiStageObserverLifecycleTest`);
});
