// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/StatePreservationRebalancingTests.cs @ v10.1.0 (MIT).
//
// Moves the rebalancer worker onto a secondary silo (via the
// `IPlacementDirector.PlacementHintKey` + `IGrainManagementExtension.MigrateOnIdle`),
// seeds a skewed load across 4 silos (same forced-placement + management-grain
// statistics surface as StaticRebalancingTests, GAP-MGMT-GRAIN), then kills the
// silo hosting the rebalancer mid-session and asserts a new leader is elected
// elsewhere and rebalancing resumes with activation state preserved. Beyond
// the statistics/placement-hint gap, this also needs a cluster-wide system
// target (`GetSystemTarget<IActivationRebalancerMonitor>`) that reports the
// *same* elected host no matter which silo is asked; this framework's
// `ActivationRebalancerWorker.report()` is per-silo local (`host` is always
// "this silo's own address", not the cluster's elected leader), so there is no
// way to discover the elected leader address from an arbitrary silo
// (GAP-REBALANCER-CONTROL).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRebalancingTests.StatePreservationRebalancingTests", () => {
  orleansTest.gap(
    "GAP-REBALANCER-CONTROL",
    "UnitTests.ActivationRebalancingTests.StatePreservationRebalancingTests.Should_Migrate_And_Preserve_State_When_Hosting_Silo_Dies",
  );
});
