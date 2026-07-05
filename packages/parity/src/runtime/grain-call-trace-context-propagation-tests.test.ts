// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainCallTraceContextPropagationTests.cs @ v10.1.0 (MIT).
// Every case asserts on `System.Diagnostics.Activity` trace-context
// propagation (trace IDs, parent/child span relationships, `traceparent`/
// `tracestate` in RequestContext, malformed-header handling, cross-silo
// propagation) around grain calls and activation. Same missing tracing/
// telemetry instrumentation as ActivationTracingTests.cs (GAP-TRACING).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.GrainCallTraceContextPropagationTests", () => {
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ServerSideGrainCallSharesSameTraceIdAsClient",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ServerSideActivityHasCorrectKindAndRemoteParent",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.NestedGrainCallsShareSameTraceId",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceParentIsSetInRequestContext",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ClientAndServerSpansAreProperlyLinked",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ServerCreatesOwnTraceWhenClientHasNoActivity",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ConcurrentGrainCallsShareSameTraceId",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ServerHandlesMalformedTraceParentGracefully",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceParentReflectsClientOutgoingSpan",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceContextIsPropagatedDuringActivation",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceStateIsPropagatedWithTraceParent",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ActivationSpanSharesTraceIdWithTriggeringGrainCall",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.FullTraceHierarchyIsCorrectWhenActivationTriggeredByGrainCall",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.DiagnoseTraceContextInRequestContextData",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ActivationStartsNewTraceWhenCallerHasNoActivity",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.GrainToGrainCallWithoutActivityContext",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.ActivationSpanHasNoParentWhenApplicationSourceNotSampled",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.DiagnoseActivitySourceSampling",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceContextPropagatedFromHttpActivityToGrainCall",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceContextPreservedWhenGrainPlacedOnDifferentSilo",
  );
  orleansTest.gap(
    "GAP-TRACING",
    "UnitTests.General.GrainCallTraceContextPropagationTests.TraceContextPreservedInCrossSiloGrainToGrainCall",
  );
});
