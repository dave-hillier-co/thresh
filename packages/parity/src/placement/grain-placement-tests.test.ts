// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/GrainPlacementTests.cs @ v10.1.0 (MIT).
//
// Upstream observes placement via `GetRuntimeInstanceId()`/`GetEndpoint()`, a
// grain reporting its own hosting silo address — now available via
// `GrainRuntime.localSiloAddress()`, so the ported tests below call the
// grain's own `getRuntimeInstanceId()` directly, same as upstream (in place
// of `GetEndpoint()`'s `IPEndPoint`, which has no equivalent here).
//
// The two stateless-worker tests were gapped under GAP-STATELESS-WORKER
// pending a test grain; core stateless-worker enforcement (catalog scaling to
// `maxLocalWorkers`, routing without a directory lookup) was already done —
// see `packages/runtime/src/runtime/stateless-worker-activation-tests.test.ts`.
// `IStatelessWorkerPlacementTestGrain`/`IOtherStatelessWorkerPlacementTestGrain`
// (`packages/parity/src/grains/impl/placement-test-grain.ts`) now exist, so
// both are ported below.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  IOtherStatelessWorkerPlacementTestGrain,
  IPreferLocalPlacementTestGrain,
  IRandomPlacementTestGrain,
  IStatelessWorkerPlacementTestGrain,
  OtherStatelessWorkerPlacementTestGrain,
  PreferLocalPlacementTestGrain,
  RandomPlacementTestGrain,
  StatelessWorkerPlacementTestGrain,
} from "@thresh/parity/grains/impl/placement-test-grain";
import type { IPlacementTestGrain } from "@thresh/parity/grains/interfaces/placement-test-grain-interfaces";
import { randomGuidKey } from "@thresh/parity/support/keys";

/** Upstream `GrainPlacementTests.CollectActivationIds`. */
async function collectActivationIds(
  grain: IPlacementTestGrain,
  sampleSize: number,
): Promise<string[]> {
  const tasks = Array.from({ length: sampleSize }, () => grain.getActivationId());
  return Promise.all(tasks);
}

/** Upstream `GrainPlacementTests.ActivationCount`. */
async function activationCount(grain: IPlacementTestGrain, sampleSize: number): Promise<number> {
  const activations = await collectActivationIds(grain, sampleSize);
  return new Set(activations).size;
}

describe("UnitTests.General.GrainPlacementTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: RandomPlacementTestGrain, interfaces: [IRandomPlacementTestGrain] },
        { ctor: PreferLocalPlacementTestGrain, interfaces: [IPreferLocalPlacementTestGrain] },
        {
          ctor: StatelessWorkerPlacementTestGrain,
          interfaces: [IStatelessWorkerPlacementTestGrain],
        },
        {
          ctor: OtherStatelessWorkerPlacementTestGrain,
          interfaces: [IOtherStatelessWorkerPlacementTestGrain],
        },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest.excluded(
    "VerifyDefaultPlacement asserts Orleans' default placement strategy is ResourceOptimizedPlacement; this framework deliberately defaults to RandomPlacement (see packages/runtime/src/placement/placement-director.ts), a documented deviation rather than a missing feature",
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

  orleansTest(
    "UnitTests.General.GrainPlacementTests.StatelessWorkerShouldCreateSpecifiedActivationCount",
    async () => {
      {
        // note: this amount should agree with both the specified minimum and
        // maximum in the StatelessWorkerPlacement declared on
        // IStatelessWorkerPlacementTestGrain (maxLocalWorkers: 1).
        const expected = 1;
        const grain = cluster.getGrain(IStatelessWorkerPlacementTestGrain, randomGuidKey());
        const actual = await activationCount(grain, expected * 50);
        expect(actual).toBeLessThanOrEqual(expected);
      }

      {
        const expected = 2;
        const grain = cluster.getGrain(IOtherStatelessWorkerPlacementTestGrain, randomGuidKey());
        const actual = await activationCount(grain, expected * 50);
        expect(actual).toBeLessThanOrEqual(expected);
      }
    },
  );

  orleansTest(
    "UnitTests.General.GrainPlacementTests.StatelessWorkerGrainShouldCreateActivationsOnLocalSilo",
    async () => {
      const sampleSize = 5;
      const key = randomGuidKey();
      const proxy = cluster.getGrain(IRandomPlacementTestGrain, randomGuidKey());
      await proxy.startLocalGrains([key]);
      const expected = await proxy.getRuntimeInstanceId();
      // Locally placed grains are multi-activation and stateless. This means
      // that we have to sample the value of the result, rather than simply
      // ask for it once, in order to get a consensus of the result.
      const actual = await proxy.sampleLocalGrainEndpoint(key, sampleSize);
      expect(actual.every((place) => place === expected)).toBe(true);
    },
  );
});
