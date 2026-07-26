// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/StatePreservationRebalancingTests.cs @ v10.1.0 (MIT).
//
// Upstream moves the rebalancer — a single `[KeepAlive, Immovable]` grain —
// onto a chosen secondary silo via `IGrainManagementExtension.MigrateOnIdle`,
// then stops that silo mid-session and asserts (via
// `GetSystemTarget<IActivationRebalancerMonitor>`, reachable from any silo)
// that a new leader is elected elsewhere and rebalancing resumes. This
// framework's rebalancer has no such migratable grain to relocate: every silo
// runs its own `ActivationRebalancerWorker`, and exactly one — the lowest
// active ring key — is deterministically elected leader from the shared
// membership view (see `@thresh/hosting`'s `activation-rebalancer.cluster.test.ts`).
// So there is nothing to "move onto a secondary silo" first; the adapted test
// below instead stops whichever silo *is* currently elected (never the
// lowest-indexed one that starts out that way, so the failover is exercised
// the same way upstream's is) and asserts the same two things upstream does:
// every silo agrees on one elected host (was GAP-REBALANCER-CONTROL — no way
// to discover the leader from an arbitrary silo; `ActivationRebalancerWorker.report()`
// now returns the elected leader's address, computed identically from
// membership on every instance, not its own), a different silo takes over
// once the old leader is gone, and rebalancing keeps converging the skew
// afterwards.
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { IManagementGrain } from "@thresh/core/management-grain";
import { RebalancingTestGrain } from "@thresh/parity/grains/impl/rebalancing-test-grain";
import { IRebalancingTestGrain } from "@thresh/parity/grains/interfaces/rebalancing-test-grain-interfaces";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";
import {
  addTestActivations,
  getActivationCount,
  settle,
} from "@thresh/parity/placement/rebalancing-tests-support";

const sessionCyclePeriodMs = 1000;

describe("UnitTests.ActivationRebalancingTests.StatePreservationRebalancingTests", () => {
  orleansTest(
    "UnitTests.ActivationRebalancingTests.StatePreservationRebalancingTests.Should_Migrate_And_Preserve_State_When_Hosting_Silo_Dies",
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

        // Every silo agrees on the same elected host, discoverable from any
        // of them — the capability GAP-REBALANCER-CONTROL closes.
        const initialHosts = cluster.silos.map((s) => s.host.getRebalancingReport()?.host);
        expect(new Set(initialHosts)).toEqual(new Set([initialHosts[0]]));
        const initialLeaderHost = initialHosts[0];
        expect(initialLeaderHost).toBeDefined();

        for (let cycle = 0; cycle < 3; cycle += 1) {
          time.advance(sessionCyclePeriodMs);
          await settle();
        }

        let stats = await mgmt.getDetailedGrainStatistics();
        const preKillCounts = new Map(
          cluster.silos.map((s) => [s.address.ringKey, getActivationCount(stats, s.address)]),
        );

        // Stop whichever silo is currently the elected leader — exercising
        // the same "the rebalancer's host dies" failover upstream drives via
        // `StopSiloAsync`, just without needing to relocate it there first.
        const leaderSilo = cluster.silos.find((s) => s.address.ringKey === initialLeaderHost);
        expect(leaderSilo).toBeDefined();
        await cluster.stopSilo(leaderSilo as TestSiloHandle);
        // `mgmt` above was bound through the (now-stopped) former primary
        // silo — `TestCluster.getGrain` resolves through `cluster.primary`
        // at call time, and stopping a silo drops it from `cluster.silos`,
        // promoting the next one to primary; re-resolve so later calls route
        // through a live silo.
        const mgmtAfterFailover = cluster.getGrain(IManagementGrain, 0n);

        // A different host is elected, and every surviving silo agrees on it.
        const newHosts = cluster.silos.map((s) => s.host.getRebalancingReport()?.host);
        expect(new Set(newHosts)).toEqual(new Set([newHosts[0]]));
        expect(newHosts[0]).toBeDefined();
        expect(newHosts[0]).not.toBe(initialLeaderHost);

        // Rebalancing resumes and keeps converging the surviving silos' skew.
        for (let cycle = 0; cycle < 40; cycle += 1) {
          time.advance(sessionCyclePeriodMs);
          await settle();
        }

        stats = await mgmtAfterFailover.getDetailedGrainStatistics();
        const survivors = cluster.silos.filter((s) => s.address.ringKey !== initialLeaderHost);
        const busiest = survivors.reduce((a, b) =>
          (preKillCounts.get(a.address.ringKey) ?? 0) > (preKillCounts.get(b.address.ringKey) ?? 0)
            ? a
            : b,
        );
        const quietest = survivors.reduce((a, b) =>
          (preKillCounts.get(a.address.ringKey) ?? 0) < (preKillCounts.get(b.address.ringKey) ?? 0)
            ? a
            : b,
        );
        expect(getActivationCount(stats, busiest.address)).toBeLessThan(
          preKillCounts.get(busiest.address.ringKey) ?? 0,
        );
        expect(getActivationCount(stats, quietest.address)).toBeGreaterThan(
          preKillCounts.get(quietest.address.ringKey) ?? 0,
        );
      } finally {
        await cluster.dispose();
      }
    },
  );
});
