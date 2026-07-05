// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/StaticRebalancingTests.cs @ v10.1.0 (MIT).
//
// This test drives a 4-silo cluster, seeds a deliberately skewed activation
// count per silo via `RequestContext.Set(IPlacementDirector.PlacementHintKey,
// silo)` + `IManagementGrain.GetDetailedGrainStatistics()`, then asserts the
// rebalancer levels the skew across silos over several session cycles. This
// framework's activation rebalancer itself exists and converges load (see
// `@tsva/hosting`'s `activation-rebalancer.cluster.test.ts`), but it exposes
// no public forced-placement hint and no management grain / per-silo
// statistics system target to observe the per-silo activation counts a test
// author (not an internal package test) can rely on (GAP-MGMT-GRAIN).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRebalancingTests.StaticRebalancingTests", () => {
  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "UnitTests.ActivationRebalancingTests.StaticRebalancingTests.Should_Move_Activations_From_Silo1_And_Silo3_To_Silo2_And_Silo4",
  );
});
