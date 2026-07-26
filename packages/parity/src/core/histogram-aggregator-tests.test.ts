// Ported from dotnet/orleans test/Orleans.Core.Tests/General/HistogramAggregatorTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// Tests Orleans.Runtime.HistogramAggregator, a custom cumulative-bucket histogram Orleans built
// for its .NET Meter-based instrumentation. This framework's metrics (packages/observability/
// src/metrics.ts) create histograms directly through @opentelemetry/api's Meter, so there is no
// equivalent bucket-collecting aggregator class to unit-test.
describe("UnitTests.General.HistogramAggregatorTests", () => {
  orleansTest.excluded(
    ".NET-specific: exercises Orleans.Runtime.HistogramAggregator's cumulative-bucket collection; this framework creates histograms directly through the OpenTelemetry SDK's Meter and has no equivalent bucket-aggregator class",
    "UnitTests.General.HistogramAggregatorTests.CollectBuckets",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
