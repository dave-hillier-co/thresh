// Ported from dotnet/orleans test/Orleans.Runtime.Tests/HeterogeneousSilosTests/UpgradeTests/UpgradeTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("Tester.HeterogeneousSilosTests.UpgradeTests", () => {
  // Every class here derives from `UpgradeTestsBase`, which builds each
  // "version" of the grain by launching a separate OS process
  // (`StandaloneSiloHandle.Create`) that loads a distinct, independently
  // published .NET assembly (`TestVersionGrains` vs. `TestVersionGrains2`) —
  // two real, differently-compiled implementations of the same grain
  // interface running in different silos, which is how Orleans exercises its
  // version-selector/compatibility-strategy placement. This harness registers
  // grains in-process from TypeScript source with a single implementation per
  // interface per process; there is no multi-assembly, multi-process silo
  // hosting mechanism to stand in for `StandaloneSiloHandle`, so none of these
  // can be ported without that DI/hosting-builder machinery.
  const reason =
    "DI/hosting builders: relies on StandaloneSiloHandle spawning separate OS processes that load distinct compiled grain assemblies (TestVersionGrains v1/v2) to simulate heterogeneous silo versions — no equivalent multi-assembly/multi-process hosting mechanism exists in this harness";

  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.MinimumVersionTests.AlwaysCreateActivationWithMinimumVersion",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.LatestVersionTests.AlwaysCreateActivationWithLatestVersion",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.LatestVersionTests.UpgradeProxyCallNoPendingRequest",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.LatestVersionTests.UpgradeProxyCallWithPendingRequest",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.AllVersionsCompatibleTests.DoNotUpgradeProxyCallNoPendingRequest",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.AllVersionsCompatibleTests.DoNotUpgradeProxyCallWithPendingRequest",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.RandomCompatibleVersionTests.CreateActivationWithBothVersion",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
