// Ported from dotnet/orleans test/Orleans.Core.Tests/Runtime/ActivationCountPlacementDirectorTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// This framework's ActivationCountPlacement (packages/runtime/src/placement/activation-count-placement.ts)
// is a power-of-k random-sampling strategy with no deployment-load-publisher stats subscription,
// no per-silo overload detection, and no SiloUnavailableException fallback chain. Every method
// below unit-tests Orleans' ActivationCountPlacementDirector via NSubstitute mocks of
// ILocalSiloDetails/IPlacementContext/IDeploymentLoadPublisher, none of which this framework has.
describe("UnitTests.Runtime.ActivationCountPlacementDirectorTests", () => {
  orleansTest.excluded(
    ".NET-specific: NSubstitute-mocked IPlacementContext/ILocalSiloDetails driving Orleans' ActivationCountPlacementDirector, which this framework's power-of-k ActivationCountPlacement does not implement (no compatible-silo cache)",
    "UnitTests.Runtime.ActivationCountPlacementDirectorTests.OnAddActivation_WhenCacheIsEmptyAndLocalSiloIsIncompatible_PlacesOnCompatibleSilo",
  );

  orleansTest.excluded(
    ".NET-specific: NSubstitute-mocked IPlacementContext/ILocalSiloDetails driving Orleans' ActivationCountPlacementDirector, which this framework's power-of-k ActivationCountPlacement does not implement (no compatible-silo cache)",
    "UnitTests.Runtime.ActivationCountPlacementDirectorTests.OnAddActivation_WhenCacheIsEmptyAndLocalSiloIsCompatible_PlacesLocally",
  );

  orleansTest.excluded(
    ".NET-specific: relies on SiloStatisticsChangeNotification from IDeploymentLoadPublisher, a stats-propagation mechanism this framework's placement strategy does not have",
    "UnitTests.Runtime.ActivationCountPlacementDirectorTests.OnAddActivation_WhenSomeCompatibleSilosHaveNoStats_PrefersSilosWithStats",
  );

  orleansTest.excluded(
    ".NET-specific: relies on SiloStatisticsChangeNotification-driven overload detection throwing SiloUnavailableException; this framework's placement strategy has no overload/exception fallback chain",
    "UnitTests.Runtime.ActivationCountPlacementDirectorTests.OnAddActivation_WhenAllCompatibleSilosWithStatsAreOverloaded_Throws",
  );

  orleansTest.excluded(
    ".NET-specific: relies on SiloStatisticsChangeNotification-driven overload detection and fallback-to-silos-without-stats; this framework's placement strategy has no such fallback chain",
    "UnitTests.Runtime.ActivationCountPlacementDirectorTests.OnAddActivation_WhenSilosWithStatsAreOverloadedAndWithoutStatsExist_FallsBackToWithoutStats",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
