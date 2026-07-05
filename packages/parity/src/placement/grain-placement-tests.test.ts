// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/GrainPlacementTests.cs @ v10.1.0 (MIT).
//
// Upstream observes placement via `GetRuntimeInstanceId()`/`GetEndpoint()`, a
// grain reporting its own hosting silo address — now available via
// `GrainRuntime.localSiloAddress()`, so the ported tests below call the
// grain's own `getRuntimeInstanceId()` directly, same as upstream.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  IPreferLocalPlacementTestGrain,
  IRandomPlacementTestGrain,
  PreferLocalPlacementTestGrain,
  RandomPlacementTestGrain,
} from "@tsva/parity/grains/impl/placement-test-grain";
import { randomGuidKey } from "@tsva/parity/support/keys";

describe("UnitTests.General.GrainPlacementTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: RandomPlacementTestGrain, interfaces: [IRandomPlacementTestGrain] },
        { ctor: PreferLocalPlacementTestGrain, interfaces: [IPreferLocalPlacementTestGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest.gap(
    "GAP-PLACEMENT-INTROSPECTION",
    "UnitTests.General.GrainPlacementTests.VerifyDefaultPlacement",
  );

  orleansTest(
    "UnitTests.General.GrainPlacementTests.RandomlyPlacedGrainShouldPlaceActivationsRandomly",
    async () => {
      const places = new Set<string>();
      for (let i = 0; i < 20; i++) {
        const key = randomGuidKey();
        const place = await cluster.getGrain(IRandomPlacementTestGrain, key).getRuntimeInstanceId();
        places.add(place);
      }
      // Consider: it seems like we should check that we get close to a 50/50
      // split for placement. Will randomly fail one in a million times if the
      // RNG is good :-)
      expect(places.size).toBeGreaterThan(1);
    },
  );

  // Upstream also has a commented-out PreferLocalPlacedGrainShouldPlaceActivationsLocally_OneHop
  // ([Fact] itself commented out); it is dead code, not an active test method.

  orleansTest(
    "UnitTests.General.GrainPlacementTests.PreferLocalPlacedGrainShouldPlaceActivationsLocally_TwoHops",
    async () => {
      const numGrains = 20;
      const randomKeys = Array.from({ length: numGrains }, () => randomGuidKey());

      const randomPlaces: string[] = [];
      for (const key of randomKeys) {
        randomPlaces.push(
          await cluster.getGrain(IRandomPlacementTestGrain, key).getRuntimeInstanceId(),
        );
      }

      const preferLocalPlaces: string[] = [];
      for (const key of randomKeys) {
        // Upstream: `grain.StartPreferLocalGrain(grain.GetPrimaryKey())` — the
        // random grain's own key doubles as the prefer-local grain's key.
        await cluster.getGrain(IRandomPlacementTestGrain, key).startPreferLocalGrain(key);
        preferLocalPlaces.push(
          await cluster.getGrain(IPreferLocalPlacementTestGrain, key).getRuntimeInstanceId(),
        );
      }

      // Check that every "prefer local grain" was placed on the same silo with
      // its requesting random grain.
      for (let i = 0; i < numGrains; i++) {
        expect(preferLocalPlaces[i]).toBe(randomPlaces[i]);
      }
    },
  );

  orleansTest.gap(
    "GAP-STATELESS-WORKER",
    "UnitTests.General.GrainPlacementTests.StatelessWorkerShouldCreateSpecifiedActivationCount",
  );

  orleansTest.gap(
    "GAP-STATELESS-WORKER",
    "UnitTests.General.GrainPlacementTests.StatelessWorkerGrainShouldCreateActivationsOnLocalSilo",
  );
});
