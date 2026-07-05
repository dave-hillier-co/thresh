// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/CustomToleranceTests.cs @ v10.1.0 (MIT).
//
// Drives the same activation-repartitioning protocol as `DefaultToleranceTests`
// but with a custom `ActivationMigrationCountLimit`/tolerance configuration,
// asserting the repartitioner eventually converts every remote call into a
// local one while respecting the configured tolerance for "acceptable" remote
// calls. This framework has no repartitioning subsystem at all
// (GAP-ACTIVATION-REPARTITIONING, see `default-tolerance-tests.test.ts`).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRepartitioningTests.CustomToleranceTests", () => {
  orleansTest.gap(
    "GAP-ACTIVATION-REPARTITIONING",
    "UnitTests.ActivationRepartitioningTests.CustomToleranceTests.Should_ConvertAllRemoteCalls_ToLocalCalls_WhileRespectingTolerance",
  );
});
