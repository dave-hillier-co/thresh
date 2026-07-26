// Ported from dotnet/orleans test/Orleans.Core.Tests/General/RingTests_Standalone.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// Exercises Orleans' ConsistentRingProvider directly, wired to IPEndPoint-based SiloAddress
// values and a hand-rolled FakeSiloStatusOracle. This framework's consistent-hash ring (covered
// by packages/parity/src/default-cluster/ring-range.test.ts and ring-range-collection.test.ts)
// uses a different SiloAddress model (Kubernetes pod identity) and a different ring
// implementation entirely, so there is no ConsistentRingProvider to unit-test the same way.
describe("UnitTests.General.RingTests_Standalone", () => {
  const reason =
    ".NET-specific: drives Orleans' ConsistentRingProvider over IPEndPoint-based SiloAddress and a hand-rolled ISiloStatusOracle; this framework's ring implementation (already covered by default-cluster/ring-range*.test.ts) uses a different SiloAddress model and algorithm";

  orleansTest.excluded(reason, "UnitTests.General.RingTests_Standalone.RingStandalone_Basic");
  orleansTest.excluded(reason, "UnitTests.General.RingTests_Standalone.RingStandalone_Failures");
  orleansTest.excluded(reason, "UnitTests.General.RingTests_Standalone.RingStandalone_Joins");
  orleansTest.excluded(reason, "UnitTests.General.RingTests_Standalone.RingStandalone_Mixed");

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
