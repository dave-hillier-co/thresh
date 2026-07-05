// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/SiloRoleBasedPlacementDirectorTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  ISiloRoleBasedPlacementGrain,
  SiloRoleBasedPlacementGrain,
} from "@tsva/parity/grains/impl/silo-role-based-placement-grain";

describe("DefaultCluster.Tests.General.SiloRoleBasedPlacementDirectorTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: SiloRoleBasedPlacementGrain, interfaces: [ISiloRoleBasedPlacementGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.General.SiloRoleBasedPlacementDirectorTests.SiloRoleBasedPlacementDirector_CantFindSilo",
    async () => {
      // No TestCluster silo advertises any role, so the grain's fixed role
      // ("Sibyl.Silo") never resolves, matching upstream's "invalid role name"
      // placement failure.
      const grain = cluster.getGrain(ISiloRoleBasedPlacementGrain, "Sibyl.Silo");
      await expect(grain.ping()).rejects.toThrow();
    },
  );

  orleansTest.gap(
    "GAP-SILO-ROLE-CONFIG",
    "DefaultCluster.Tests.General.SiloRoleBasedPlacementDirectorTests.SiloRoleBasedPlacementDirector_CanFindSilo",
  );
});
