// Ported from dotnet/orleans test/Orleans.Core.Tests/General/CounterAggregatorGroupTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// Tests Orleans.Runtime.CounterAggregatorGroup, a custom in-process metrics-aggregation cache
// Orleans built for its .NET Meter-based instrumentation. This framework's metrics
// (packages/observability/src/metrics.ts) delegate directly to the @opentelemetry/api SDK's own
// Counter/Histogram instruments instead of maintaining a custom aggregator-group cache, so
// there is no counterpart class to unit-test.
describe("UnitTests.General.CounterAggregatorGroupTests", () => {
  const reason =
    ".NET-specific: exercises Orleans.Runtime.CounterAggregatorGroup, a custom metrics-aggregation cache; this framework delegates metrics directly to the OpenTelemetry SDK's own instruments and has no equivalent aggregator-group class";

  orleansTest.excluded(
    reason,
    "UnitTests.General.CounterAggregatorGroupTests.ValidateAggregatorCache",
  );
  orleansTest.excluded(reason, "UnitTests.General.CounterAggregatorGroupTests.Collect");
  orleansTest.excluded(
    ".NET-specific: stress-tests CounterAggregatorGroup's thread-safety under Parallel.For across Environment.ProcessorCount threads; this framework's metrics delegate to the OpenTelemetry SDK and run single-threaded, with no equivalent aggregator class to race",
    "UnitTests.General.CounterAggregatorGroupTests.TestMultithreadedCorrectness",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
