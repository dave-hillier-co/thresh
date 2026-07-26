// Ported from dotnet/orleans test/Orleans.Core.Tests/OrleansRuntime/Streams/RoundRobinSelectorTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("UnitTests.OrleansRuntime.Streams.RoundRobinSelectorTests", () => {
  orleansTest.excluded(
    "internal bin-packing/round-robin queue-balancing algorithm used by Orleans' persistent-stream pulling agent; this framework's queue ownership uses a different consistent-hash-ring design (packages/streams/src/queue-ownership.ts) with no equivalent selector/balancer algorithm",
    "UnitTests.OrleansRuntime.Streams.RoundRobinSelectorTests.NextSelectionWillGoThroughEveryResourceIfExistingSelectionEmptyTest",
  );
  orleansTest.excluded(
    "internal bin-packing/round-robin queue-balancing algorithm used by Orleans' persistent-stream pulling agent; this framework's queue ownership uses a different consistent-hash-ring design (packages/streams/src/queue-ownership.ts) with no equivalent selector/balancer algorithm",
    "UnitTests.OrleansRuntime.Streams.RoundRobinSelectorTests.NextSelectionWontGoInfinitelyTest",
  );
  orleansTest.excluded(
    "internal bin-packing/round-robin queue-balancing algorithm used by Orleans' persistent-stream pulling agent; this framework's queue ownership uses a different consistent-hash-ring design (packages/streams/src/queue-ownership.ts) with no equivalent selector/balancer algorithm",
    "UnitTests.OrleansRuntime.Streams.RoundRobinSelectorTests.NextSelectionWontReSelectExistingSelectionsTest",
  );
  orleansTest.excluded(
    "internal bin-packing/round-robin queue-balancing algorithm used by Orleans' persistent-stream pulling agent; this framework's queue ownership uses a different consistent-hash-ring design (packages/streams/src/queue-ownership.ts) with no equivalent selector/balancer algorithm",
    "UnitTests.OrleansRuntime.Streams.RoundRobinSelectorTests.NextSelectionWontReSelectExistingSelectionsWithDuplicatesTest",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
