// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryStreamBatchingTests.cs @ v10.1.0 (MIT).
// Base class: test/Orleans.Streaming.Tests/StreamingTests/StreamBatchingTestRunner.cs @ v10.1.0 (MIT).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.StreamingTests.MemoryStreamBatchingTests", () => {
  orleansTest.excluded(
    "skipped upstream (dotnet/orleans#5649)",
    "UnitTests.StreamingTests.MemoryStreamBatchingTests.SingleSendBatchConsume",
  );

  // Requires a batch-send API (`IAsyncStream.OnNextBatchAsync`, delivering
  // several items to a consumer in one pulling-agent batch) and a configurable
  // pulling-agent batch container size. `AsyncStream` here only exposes
  // `publish(event)` for a single item at a time, so there is no way to send
  // (or observe delivery of) a batch.
  orleansTest.gap(
    "GAP-STREAM-BATCHING",
    "UnitTests.StreamingTests.MemoryStreamBatchingTests.BatchSendSingleConsume",
  );

  orleansTest.excluded(
    "skipped upstream (dotnet/orleans#5632)",
    "UnitTests.StreamingTests.MemoryStreamBatchingTests.BatchSendBatchConsume",
  );
});
