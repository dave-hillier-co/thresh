// Ported from dotnet/orleans test/Orleans.Runtime.Tests/HeterogeneousSilosTests/UpgradeTests/VersionPlacementTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("Tester.HeterogeneousSilosTests.UpgradeTests.VersionPlacementTests", () => {
  // Same `UpgradeTestsBase` machinery as UpgradeTests.cs: distinct silo
  // "versions" are separate OS processes loading distinct compiled grain
  // assemblies (`StandaloneSiloHandle` + `TestVersionGrains`/`TestVersionGrains2`).
  // No multi-assembly/multi-process silo hosting mechanism exists in this
  // harness to stand in for that.
  orleansTest.excluded(
    "DI/hosting builders: relies on StandaloneSiloHandle spawning separate OS processes that load distinct compiled grain assemblies (TestVersionGrains v1/v2) to simulate heterogeneous silo versions — no equivalent multi-assembly/multi-process hosting mechanism exists in this harness",
    "Tester.HeterogeneousSilosTests.UpgradeTests.VersionPlacementTests.ActivateDominantVersion",
  );

  // This file's single upstream Fact is entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
