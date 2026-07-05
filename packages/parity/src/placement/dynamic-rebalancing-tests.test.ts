// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/DynamicRebalancingTests.cs @ v10.1.0 (MIT).
//
// Same shape as StaticRebalancingTests but keeps creating new activations
// (forced onto specific silos via the placement hint) while rebalancing
// cycles run, then checks the final per-silo counts against
// `IManagementGrain.GetDetailedGrainStatistics()`. Needs the same forced-
// placement hint and management-grain statistics surface StaticRebalancingTests
// does, which this framework does not expose (GAP-MGMT-GRAIN); see that file's
// header for what does exist (the rebalancer itself converges load).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRebalancingTests.DynamicRebalancingTests", () => {
  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "UnitTests.ActivationRebalancingTests.DynamicRebalancingTests.Should_Move_Activations_From_Silo1_And_Silo3_To_Silo2_And_Silo4_While_New_Activations_Are_Created",
  );
});
