// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/StatelessWorkersStreamTests.cs @ v10.1.0 (MIT).
//
// Asserts that a stateless-worker grain (`IStatelessWorkerStreamConsumerGrain`,
// `[StatelessWorker]`) throws `InvalidOperationException` when it tries to
// become a stream consumer — Orleans forbids subscribing from a grain that
// may have multiple concurrent activations, since a stream subscription must
// bind to a single activation. This framework's `StatelessWorker` placement
// (`packages/runtime/src/placement/stateless-worker-placement.ts`) is parsed
// but not enforced — every other stateless-worker parity test is gapped under
// `GAP-STATELESS-WORKER` (see `packages/parity/src/runtime/stateless-worker-activation-tests.test.ts`)
// because the runtime never actually creates multiple concurrent activations
// for one, and there is no validation path that rejects a stream subscription
// from one — so there is nothing here to throw the invariant this test checks.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-STATELESS-WORKER",
  "UnitTests.StreamingTests.StatelessWorkersStreamTests.SubscribeToStream_FromStatelessWorker_Fail",
);

// vitest requires at least one runtime test per file; the sole upstream Fact
// is a gap above, so this placeholder keeps the file a valid suite.
it.skip("(the sole test in this file is orleansTest.gap — see above)", () => undefined);
