// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/SiloRoleBasedPlacementDirectorTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  ISiloRoleBasedPlacementGrain,
  SiloRoleBasedPlacementGrain,
} from "@thresh/parity/grains/impl/silo-role-based-placement-grain";

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
});

describe("DefaultCluster.Tests.General.SiloRoleBasedPlacementDirectorTests (role advertised)", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: SiloRoleBasedPlacementGrain, interfaces: [ISiloRoleBasedPlacementGrain] }],
      // Silo 1 advertises the role the grain is fixed to at registration
      // (`@grain({ placement: "siloRole", role: "Sibyl.Silo" })`), the way an
      // upstream silo's own name resolves the target role at placement time.
      siloMetadata: ({ index }) => (index === 1 ? { role: "Sibyl.Silo" } : undefined),
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.General.SiloRoleBasedPlacementDirectorTests.SiloRoleBasedPlacementDirector_CanFindSilo",
    async () => {
      const grain = cluster.getGrain(ISiloRoleBasedPlacementGrain, "testhost");
      const result = await grain.ping();
      expect(result).toBe(true);
    },
  );
});
