// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/DynamicRebalancingTests.cs @ v10.1.0 (MIT).
//
// Same shape as `static-rebalancing-tests.test.ts` but keeps creating new
// activations (forced onto specific silos via the placement hint) while
// rebalancing cycles run, then checks the final per-silo counts against
// `IManagementGrain.getDetailedGrainStatistics()`. See that file's header for
// the same fake-clock/tick-count adaptation this test makes.
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

describe("UnitTests.ActivationRebalancingTests.DynamicRebalancingTests", () => {
  orleansTest(
    "UnitTests.ActivationRebalancingTests.DynamicRebalancingTests.Should_Move_Activations_From_Silo1_And_Silo3_To_Silo2_And_Silo4_While_New_Activations_Are_Created",
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

        // Extra activations, one tenth of the initial count for each silo,
        // added on every other cycle (upstream's `index % 2 == 0`).
        const extraSilo1 = 30;
        const extraSilo2 = 3;
        const extraSilo3 = 18;
        const extraSilo4 = 10;
        let extraRounds = 0;

        let silo1Count = initialSilo1;
        let silo2Count = initialSilo2;
        let silo3Count = initialSilo3;
        let silo4Count = initialSilo4;

        for (let cycle = 0; cycle < 40; cycle += 1) {
          time.advance(sessionCyclePeriodMs);
          await settle();

          if (cycle % 2 === 0) {
            const extra: Promise<unknown>[] = [];
            addTestActivations(cluster, extra, silo1!, extraSilo1);
            addTestActivations(cluster, extra, silo2!, extraSilo2);
            addTestActivations(cluster, extra, silo3!, extraSilo3);
            addTestActivations(cluster, extra, silo4!, extraSilo4);
            await Promise.all(extra);
            await settle();
            extraRounds += 1;
          }

          stats = await mgmt.getDetailedGrainStatistics();
          silo1Count = getActivationCount(stats, silo1!);
          silo2Count = getActivationCount(stats, silo2!);
          silo3Count = getActivationCount(stats, silo3!);
          silo4Count = getActivationCount(stats, silo4!);
        }

        const finalSilo1 = initialSilo1 + extraRounds * extraSilo1;
        const finalSilo2 = initialSilo2 + extraRounds * extraSilo2;
        const finalSilo3 = initialSilo3 + extraRounds * extraSilo3;
        const finalSilo4 = initialSilo4 + extraRounds * extraSilo4;

        expect(silo1Count).toBeLessThan(finalSilo1);
        expect(silo2Count).toBeGreaterThan(finalSilo2);
        expect(silo3Count).toBeLessThan(finalSilo3);
        expect(silo4Count).toBeGreaterThan(finalSilo4);
      } finally {
        await cluster.dispose();
      }
    },
  );
});
