// Ported from dotnet/orleans test/Orleans.Runtime.Tests/HeterogeneousSilosTests/UpgradeTests/RuntimeStrategyChangeTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.HeterogeneousSilosTests.UpgradeTests.RuntimeStrategyChangeTests", () => {
  // Same `UpgradeTestsBase` out-of-process, multi-assembly silo machinery as
  // UpgradeTests.cs, plus `IManagementGrain.SetCompatibilityStrategy` /
  // `SetSelectorStrategy` (GAP-MGMT-GRAIN: no management grain / silo-control
  // system targets exist here) to change versioning strategy at runtime.
  const reason =
    "DI/hosting builders + GAP-MGMT-GRAIN: relies on StandaloneSiloHandle spawning separate OS processes loading distinct compiled grain assemblies (TestVersionGrains v1/v2), and on IManagementGrain.Set{Compatibility,Selector}Strategy (no management grain exists in this harness) — neither has an equivalent surface here";

  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.RuntimeStrategyChangeTests.ChangeCompatibilityStrategy",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.RuntimeStrategyChangeTests.ChangeVersionSelectorStrategy",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.RuntimeStrategyChangeTests.ChangeDefaultVersionCompatibilityStrategy",
  );
  orleansTest.excluded(
    reason,
    "Tester.HeterogeneousSilosTests.UpgradeTests.RuntimeStrategyChangeTests.ChangeDefaultVersionSelectorStrategy",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
