// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/GrainPlacementClusterChangeTests.cs @ v10.1.0 (MIT).
//
// This test kills a silo mid-test, so (per convention) it gets its own
// cluster per case rather than sharing one across the describe block.
import { describe, expect } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import { getGrainMetadata } from "@tsva/core/grain-metadata";
import type { Guid } from "@tsva/core/guid";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster, type TestSiloHandle } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";
import {
  IPreferLocalPlacementTestGrain,
  IRandomPlacementTestGrain,
  PreferLocalPlacementTestGrain,
  RandomPlacementTestGrain,
} from "@tsva/parity/grains/impl/placement-test-grain";
import { randomGuidKey } from "@tsva/parity/support/keys";

const randomGrainType = getGrainMetadata(RandomPlacementTestGrain)!.grainType;
const preferLocalGrainType = getGrainMetadata(PreferLocalPlacementTestGrain)!.grainType;

function hostOf(cluster: TestCluster, grainId: GrainId): TestSiloHandle | undefined {
  return cluster.silos.find((s) => s.host.isActive(grainId));
}

describe("UnitTests.General.GrainPlacementClusterChangeTests", () => {
  orleansTest.each(["Primary", "Secondary"] as const)(
    "UnitTests.General.GrainPlacementClusterChangeTests.PreferLocalPlacementGrain_ShouldMigrateWhenHostSiloKilled",
    async (value) => {
      const cluster = await TestCluster.start({
        initialSilos: 2,
        grains: [
          { ctor: RandomPlacementTestGrain, interfaces: [IRandomPlacementTestGrain] },
          { ctor: PreferLocalPlacementTestGrain, interfaces: [IPreferLocalPlacementTestGrain] },
        ],
      });
      try {
        const targetSilo = value === "Primary" ? cluster.silos[0]! : cluster.silos[1]!;

        // Upstream: `do { ... } while (!targetSilo.Equals(expected));` — retry
        // random keys until the proxy lands on the target silo.
        let proxyKey!: Guid;
        for (;;) {
          proxyKey = randomGuidKey();
          await cluster.getGrain(IRandomPlacementTestGrain, proxyKey).nop();
          if (hostOf(cluster, new GrainId(randomGrainType, proxyKey)) === targetSilo) break;
        }

        // Prefer-local placement, invoked from within the proxy's own
        // activation, lands on the proxy's host: the target silo.
        await cluster.getGrain(IRandomPlacementTestGrain, proxyKey).startPreferLocalGrain(proxyKey);
        const expected = hostOf(cluster, new GrainId(preferLocalGrainType, proxyKey));
        expect(expected).toBe(targetSilo); // PreferLocalPlacement strategy should create activations on the local silo.

        await cluster.killSilo(targetSilo);

        // The killed silo's activation reactivates (fresh) on a survivor on
        // next call.
        await waitFor(async () => {
          await cluster.getGrain(IPreferLocalPlacementTestGrain, proxyKey).nop();
          return true;
        });
        const newActual = hostOf(cluster, new GrainId(preferLocalGrainType, proxyKey));
        // PreferLocalPlacement strategy should recreate activations on other silo if local fails.
        expect(newActual).not.toBe(expected);
      } finally {
        await cluster.dispose();
      }
    },
  );
});
