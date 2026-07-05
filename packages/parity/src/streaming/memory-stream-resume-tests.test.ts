// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryStreamResumeTests.cs @ v10.1.0 (MIT).
// Base class: test/Orleans.Streaming.Tests/StreamingTests/StreamingResumeTests.cs @ v10.1.0 (MIT).
//
// All five inherited [SkippableFact]s exercise the memory stream provider's
// pulling-agent cache and inactivity model: `StreamInactivityPeriod` (silo
// config), cache-eviction options (`MetadataMinTimeInCache`,
// `DataMaxAgeInCache`, `DataMinTimeInCache`), and
// `StreamingDiagnosticObserver` — a test-only hook into the pulling agent's
// internal cache/inactivity telemetry (`WaitForItemDeliveryCountAsync`,
// `WaitForStreamInactiveAsync`). This framework's `MemoryStreamProvider` is a
// direct push-based fan-out over an unbounded in-memory event log per stream
// (packages/streams/src/memory-stream-provider.ts): there is no pulling
// agent, no cache, no stream-inactivity timer, and so no notion of a stream
// "going inactive" or being "resumed" after cache eviction — every
// subscription simply replays from its cursor forever. There is also no
// diagnostic-observer equivalent to synchronize on that internal state.
import { orleansTest } from "@tsva/testing/orleans-test";

// Tester.StreamingTests.MemoryStreamResumeTests
orleansTest.gap(
  "GAP-STREAM-CACHE-DIAGNOSTICS",
  "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterInactivity",
);

orleansTest.gap(
  "GAP-STREAM-CACHE-DIAGNOSTICS",
  "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterInactivityNotInCache",
);

orleansTest.gap(
  "GAP-STREAM-CACHE-DIAGNOSTICS",
  "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterDeactivation",
);

orleansTest.gap(
  "GAP-STREAM-CACHE-DIAGNOSTICS",
  "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterDeactivationActiveStream",
);

orleansTest.gap(
  "GAP-STREAM-CACHE-DIAGNOSTICS",
  "Tester.StreamingTests.MemoryStreamResumeTests.ResumeAfterSlowSubscriber",
);
