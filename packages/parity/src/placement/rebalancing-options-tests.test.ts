// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/RebalancingOptionsTests.cs @ v10.1.0 (MIT).
//
// `ConstantsShouldNotChange` pins the exact numeric defaults on
// `ActivationRebalancerOptions` (including `DEFAULT_REBALANCER_DUE_TIME`, a
// startup-delay field, and the standalone `MAX_SCALED_ENTROPY_DEVIATION`
// constant); `InvalidOptionsShouldThrow` exercises
// `ActivationRebalancerOptionsValidator.ValidateConfiguration` across 8 bad
// combinations of `SessionCyclePeriod`, `MaxStagnantCycles`,
// `EntropyQuantum`, `AllowedEntropyDeviation`, `CycleNumberWeight`,
// `SiloNumberWeight`, `ActivationMigrationCountLimit`, and
// `ScaledEntropyDeviationActivationThreshold` against
// `DeploymentLoadPublisherOptions.DeploymentLoadPublisherRefreshTime`. This
// framework's `DEFAULT_REBALANCER_OPTIONS` (`@tsva/runtime/placement/
// rebalancing/rebalancer-model`) has no due-time field (there is no
// grain-hosted startup delay to configure) and inlines the "max scaled
// entropy deviation" as a literal rather than a named export, and there is no
// `ActivationRebalancerOptionsValidator` (or any options-validation layer) at
// all for the rebalancer (GAP-REBALANCER-CONTROL).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRebalancingTests.RebalancingOptionsTests", () => {
  orleansTest.gap(
    "GAP-REBALANCER-CONTROL",
    "UnitTests.ActivationRebalancingTests.RebalancingOptionsTests.ConstantsShouldNotChange",
  );

  orleansTest.gap(
    "GAP-REBALANCER-CONTROL",
    "UnitTests.ActivationRebalancingTests.RebalancingOptionsTests.InvalidOptionsShouldThrow",
  );
});
