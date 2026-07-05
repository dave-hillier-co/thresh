// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ActivityPropagationTests.cs @ v10.1.0 (MIT).
// Verifies `ActivityPropagationGrainCallFilter` propagates
// `System.Diagnostics.Activity` trace context (W3C and Hierarchical ID
// formats, trace state, baggage) from client to grain across grain calls,
// requiring `.AddActivityPropagation()` on both silo and client builders.
// Same missing tracing/telemetry instrumentation as ActivationTracingTests.cs
// (GAP-TRACING) — this framework has no Activity/ActivitySource-equivalent
// trace-context propagation mechanism.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.ActivityPropagationTests", () => {
  // [Theory] over ActivityIdFormat.{W3C, Hierarchical} — one upstream id for both cases.
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivityPropagationTests.WithoutParentActivity",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivityPropagationTests.WithParentActivity_W3C",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.ActivityPropagationTests.WithParentActivity_Hierarchical",
  );
});
