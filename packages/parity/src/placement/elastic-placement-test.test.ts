// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/ElasticPlacementTest.cs @ v10.1.0 (MIT).
//
// The four elasticity tests are `[SkippableFact(Skip = "...")]` upstream
// (https://github.com/dotnet/orleans/issues/4008) — skipped there too. The
// remaining two exercise load-aware placement avoiding an overloaded/busy
// silo, which needs an `OverloadDetector`/CPU-usage-aware environment
// statistics subsystem this framework does not have: placement strategies
// here only weigh activation counts (GAP-LOAD-SHEDDING).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.ElasticPlacementTests", () => {
  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_CatchingUp",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_StoppingSilos",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_AllSilosCPUTooHigh",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_AllSilosOverloaded",
  );

  orleansTest.gap(
    "GAP-LOAD-SHEDDING",
    "UnitTests.General.ElasticPlacementTests.LoadAwareGrainShouldNotAttemptToCreateActivationsOnOverloadedSilo",
  );

  orleansTest.gap(
    "GAP-LOAD-SHEDDING",
    "UnitTests.General.ElasticPlacementTests.LoadAwareGrainShouldNotAttemptToCreateActivationsOnBusySilos",
  );
});
