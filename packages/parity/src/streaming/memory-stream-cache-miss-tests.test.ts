// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryStreamCacheMissTests.cs @ v10.1.0 (MIT).
// Base class: test/Orleans.Streaming.Tests/StreamingTests/StreamingCacheMissTests.cs @ v10.1.0 (MIT).
//
// Both inherited [SkippableFact]s rely on the pulling-agent cache-eviction
// model (`DataMaxAgeInCache` / `DataMinTimeInCache` silo config) and
// `StreamingDiagnosticObserver` for cache/delivery telemetry — see
// memory-stream-resume-tests.test.ts for why that infrastructure does not
// exist here. `PreviousEventEvictedFromCacheWithFilterTest` additionally
// needs `IStreamFilter` / `ISiloBuilder.AddStreamFilter`, a server-side
// predicate that suppresses delivery of non-matching items before they reach
// a consumer; this framework's stream providers have no filtering hook at
// all (every published event reaches every subscriber).
import { orleansTest } from "@tsva/testing/orleans-test";

// Tester.StreamingTests.MemoryStreamCacheMissTests
orleansTest.gap(
  "GAP-STREAM-CACHE-DIAGNOSTICS",
  "Tester.StreamingTests.MemoryStreamCacheMissTests.PreviousEventEvictedFromCacheTest",
);

orleansTest.gap(
  "GAP-STREAM-FILTER",
  "Tester.StreamingTests.MemoryStreamCacheMissTests.PreviousEventEvictedFromCacheWithFilterTest",
);
