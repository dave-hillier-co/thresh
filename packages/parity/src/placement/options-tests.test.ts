// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/OptionsTests.cs @ v10.1.0 (MIT).
//
// `ConstantsShouldNotChange` pins the exact numeric defaults on
// `ActivationRepartitionerOptions` (`DEFAULT_ANCHORING_FILTER_ENABLED`,
// `DEFAULT_PROBABILISTIC_FILTERING_MAX_ALLOWED_ERROR`,
// `DEFAULT_MAX_EDGE_COUNT`, `DEFAULT_MINUMUM_ROUND_PERIOD`,
// `DEFAULT_MAXIMUM_ROUND_PERIOD`, `DEFAULT_RECOVERY_PERIOD`);
// `InvalidOptionsShouldThrow` exercises
// `ActivationRepartitionerOptionsValidator.ValidateConfiguration` across 8 bad
// combinations of `MaxEdgeCount`, `MaxUnprocessedEdges`, `MinRoundPeriod`,
// `MaxRoundPeriod`, `RecoveryPeriod`, and
// `ProbabilisticFilteringMaxAllowedErrorRate`. This framework has no
// repartitioning subsystem at all: no `ActivationRepartitionerOptions` type
// and no options-validation layer for it (GAP-ACTIVATION-REPARTITIONING, see
// `default-tolerance-tests.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRepartitioningTests.OptionsTests", () => {
  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.OptionsTests.ConstantsShouldNotChange",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.OptionsTests.InvalidOptionsShouldThrow",
  );
});
