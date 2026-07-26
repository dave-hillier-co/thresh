// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/AsyncEnumerableGrainCallTests.cs @ v10.1.0 (MIT).
//
// This entire file exercises grain methods that return `IAsyncEnumerable<T>`,
// a distinct Orleans wire mechanism (`IAsyncEnumerableGrainExtension` with
// `StartEnumeration`/`MoveNext`/`DisposeAsync`, batching via `WithBatchSize`,
// and cooperative cancellation propagated through that extension). This
// framework's grain call surface only supports request/response `Promise<T>`
// methods (and one-way calls) — there is no streaming-result grain call, no
// per-call grain extension mechanism to model the enumerator lifecycle, and
// no batching protocol to test against. None of it is representable without
// inventing a whole new grain-call kind, so every test in this file is
// excluded rather than gapped against a single missing primitive.
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const reason =
  "IAsyncEnumerable<T> grain-method return values are a distinct Orleans wire " +
  "mechanism (IAsyncEnumerableGrainExtension: StartEnumeration/MoveNext/DisposeAsync, " +
  "batching, cooperative cancellation) with no analog in this framework's request/response " +
  "and one-way grain call surface.";

describe("DefaultCluster.Tests.AsyncEnumerableGrainCallTests", () => {
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_CancelBeforeYield",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_Throws",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_Cancellation",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_CancellationToken",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_CancellationToken_WithCancellationExtension",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_Batch",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_SplitBatch",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_NoBatching",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_WithCancellation",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_SlowProducer",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_SlowConsumer",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_SlowConsumer_Evicted",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.AsyncEnumerableGrainCallTests.ObservableGrain_AsyncEnumerable_Deactivate",
  );

  // Every upstream Fact/Theory in this file is excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
