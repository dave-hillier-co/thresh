// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/DefaultToleranceTests.cs @ v10.1.0 (MIT).
//
// Exercises the activation *repartitioner*: a background protocol that moves
// individual grain activations near their most frequent communication
// partners (tracked via a blocked-bloom-filter-backed frequent-edge counter),
// distinct from the activation *rebalancer* (load-leveling, already covered
// under `UnitTests.ActivationRebalancingTests`, GAP-REBALANCER-CONTROL). This
// framework has no repartitioning subsystem at all: no
// `IActivationRepartitioner`/`ActivationRepartitionerOptions`, no
// communication-graph tracking, no `[Immovable(ImmovableKind.Repartitioner)]`
// grain attribute, and no manually-triggerable exchange-request protocol
// (GAP-ACTIVATION-REPARTITIONING).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRepartitioningTests.DefaultToleranceTests", () => {
  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.A_ShouldMoveToSilo2__B_And_C_ShouldStayOnSilo2",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.C_ShouldMoveToSilo1__A_And_B_ShouldStayOnSilo1",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.Immovable_C_ShouldStayOnSilo2__A_And_B_ShouldMoveToSilo2",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.A_ShouldMoveToSilo2_Or_C_ShouldMoveToSilo1__B_And_D_ShouldStayOnTheirSilos",
  );

  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.DefaultToleranceTests.Receivers_ShouldMoveCloseTo_PullingAgent",
  );
});
