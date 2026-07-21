// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/StaticRebalancingTests.cs @ v10.1.0 (MIT).
//
// Drives a 4-silo cluster, seeds a deliberately skewed activation count per
// silo via `RequestContext.set(PLACEMENT_HINT_KEY, silo.toString())` +
// `IManagementGrain.getDetailedGrainStatistics()`, then asserts the
// rebalancer levels the skew across silos over several cycles. Upstream waits
// on real wall-clock delays between three `SessionCyclePeriod`-spaced checks;
// this port drives the shared `FakeTimeProvider` instead, and — since this
// framework's rebalancer runs exactly one migration cycle per tick (Orleans
// runs a whole internal session, many cycles deep, between each of the three
// checks) — ticks it enough times for the same directional assertions
// upstream makes to hold, rather than reusing the literal "3" from the
// upstream loop.
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";
import { IManagementGrain } from "@tsva/core/management-grain";
import { RebalancingTestGrain } from "@tsva/parity/grains/impl/rebalancing-test-grain";
import { IRebalancingTestGrain } from "@tsva/parity/grains/interfaces/rebalancing-test-grain-interfaces";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  addTestActivations,
  getActivationCount,
  settle,
} from "@tsva/parity/placement/rebalancing-tests-support";

const sessionCyclePeriodMs = 1000;

describe("UnitTests.ActivationRebalancingTests.StaticRebalancingTests", () => {
  orleansTest(
    "UnitTests.ActivationRebalancingTests.StaticRebalancingTests.Should_Move_Activations_From_Silo1_And_Silo3_To_Silo2_And_Silo4",
    async () => {
      const time = new FakeTimeProvider();
      const cluster = await TestCluster.start({
        initialSilos: 4,
        time,
        grains: [{ ctor: RebalancingTestGrain, interfaces: [IRebalancingTestGrain] }],
        configureSilo: (builder) =>
          builder.useActivationRebalancing({
            sessionCyclePeriodSeconds: sessionCyclePeriodMs / 1000,
          }),
      });
      try {
        const [silo1, silo2, silo3, silo4] = cluster.silos.map((s) => s.address);
        const mgmt = cluster.getGrain(IManagementGrain, 0n);

        const tasks: Promise<unknown>[] = [];
        addTestActivations(cluster, tasks, silo1!, 300);
        addTestActivations(cluster, tasks, silo2!, 30);
        addTestActivations(cluster, tasks, silo3!, 180);
        addTestActivations(cluster, tasks, silo4!, 100);
        await Promise.all(tasks);
        await settle();

        let stats = await mgmt.getDetailedGrainStatistics();
        const initialSilo1 = getActivationCount(stats, silo1!);
        const initialSilo2 = getActivationCount(stats, silo2!);
        const initialSilo3 = getActivationCount(stats, silo3!);
        const initialSilo4 = getActivationCount(stats, silo4!);

        let silo1Count = initialSilo1;
        let silo2Count = initialSilo2;
        let silo3Count = initialSilo3;
        let silo4Count = initialSilo4;

        for (let cycle = 0; cycle < 40; cycle += 1) {
          time.advance(sessionCyclePeriodMs);
          await settle();
          stats = await mgmt.getDetailedGrainStatistics();
          silo1Count = getActivationCount(stats, silo1!);
          silo2Count = getActivationCount(stats, silo2!);
          silo3Count = getActivationCount(stats, silo3!);
          silo4Count = getActivationCount(stats, silo4!);
        }

        expect(silo1Count).toBeLessThan(initialSilo1);
        expect(silo2Count).toBeGreaterThan(initialSilo2);
        expect(silo3Count).toBeLessThan(initialSilo3);
        expect(silo4Count).toBeGreaterThan(initialSilo4);
      } finally {
        await cluster.dispose();
      }
    },
  );
});
