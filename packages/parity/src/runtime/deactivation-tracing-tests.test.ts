// Ported from dotnet/orleans test/Orleans.Runtime.Tests/DeactivationTracingTests.cs @ v10.1.0 (MIT).
// Every case asserts on `System.Diagnostics.Activity` spans created around
// `OnDeactivateAsync`, deactivation-triggered storage writes, migration
// dehydrate ordering, exceptions during deactivation, and trace-context
// inheritance — same missing tracing/telemetry instrumentation as
// ActivationTracingTests.cs (GAP-TRACING).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.DeactivationTracingTests", () => {
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsCreatedOnDeactivateOnIdle",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIncludesStorageWriteDuringDeactivation",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanCapturesExceptionDuringDeactivation",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanPrecedesDehydrateDuringMigration",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsNotCreatedForNonGrainBaseGrain",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanInheritsTraceContextFromTriggeringCall",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsParentedToAsyncEnumerableMethodCall",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanHasCorrectReasonTagForMigration",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsCreatedForInconsistentStateException",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsNotCreatedForActivationFailure",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanIsCreatedForGrainContextDeactivate",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.DeactivationTracingTests.OnDeactivateSpanHasCorrectParentWhenTriggeredExternally",
  );
});
