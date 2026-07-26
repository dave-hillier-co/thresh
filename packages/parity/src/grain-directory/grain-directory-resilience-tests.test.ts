// Ported from dotnet/orleans test/Orleans.GrainDirectory.Tests/GrainDirectory/GrainDirectoryResilienceTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("UnitTests.GrainDirectory.GrainDirectoryResilienceTests", () => {
  orleansTest.excluded(
    ".NET-specific Stress-category chaos test: runs for a real 5-minute wall-clock " +
      "duration (CancellationTokenSource(TimeSpan.FromMinutes(5))), continuously " +
      "starting/stopping/killing TestCluster silos while hammering the cluster with " +
      "grain calls, and directly invokes .NET-only system-target/membership " +
      "internals (IGrainDirectoryTestHooks.CheckIntegrityAsync per silo per " +
      "DirectoryMembershipSnapshot.PartitionsPerSilo partition, InProcessSiloHandle, " +
      "AddDistributedGrainDirectory) that have no equivalent here. This framework's " +
      "membership authority is Kubernetes by design (docs/deviations.md), not an " +
      "in-process TestCluster membership protocol, and the harness forbids real " +
      "sleeps/timeouts over 1s, ruling out a faithful port.",
    "UnitTests.GrainDirectory.GrainDirectoryResilienceTests.ElasticChaos",
  );

  // vitest requires at least one runtime test per file; the sole upstream Fact
  // is excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(the sole test in this file is orleansTest.excluded — see above)", () => undefined);
});
