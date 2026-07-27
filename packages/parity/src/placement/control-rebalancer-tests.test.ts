// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/ControlRebalancerTests.cs @ v10.1.0 (MIT).
//
// Exercises the activation rebalancer's control surface on `SiloHost`:
// `getRebalancingReport`, `suspendRebalancing`/`resumeRebalancing` (including a
// timed suspension that auto-resumes off the shared clock), and the
// `subscribeToReports`/`unsubscribeFromReports` report-listener pub/sub.
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { RebalancingReport } from "@thresh/runtime/placement/rebalancing/rebalancing-report";
import { TestCluster } from "@thresh/testing/test-cluster";

describe("UnitTests.ActivationRebalancingTests.ControlRebalancerTests", () => {
  orleansTest(
    "UnitTests.ActivationRebalancingTests.ControlRebalancerTests.Rebalancer_Should_Be_Controllable_And_Report_To_Listeners",
    async () => {
      const time = new FakeTimeProvider();
      const cluster = await TestCluster.start({
        initialSilos: 1,
        time,
        configureSilo: (builder) =>
          builder.useActivationRebalancing({ sessionCyclePeriodSeconds: 1 }),
      });
      try {
        const host = cluster.primary.host;

        const report = host.getRebalancingReport();
        const rebalancerHost = report?.host;

        expect(report?.status).toBe("executing");
        expect(report?.suspensionDurationMs).toBeUndefined();
        expect(rebalancerHost).not.toBeUndefined();

        // Publish-Subscribe
        let received: RebalancingReport | undefined;
        const listener = (r: RebalancingReport) => {
          received = r;
        };
        host.subscribeToReports(listener);
        expect(received).toBeUndefined();

        host.resumeRebalancing();
        expect(received).not.toBeUndefined();
        expect(received?.status).toBe("executing");
        expect(received?.host).toBe(rebalancerHost);

        host.suspendRebalancing();
        expect(received?.status).toBe("suspended");
        expect(received?.suspensionDurationMs).not.toBeUndefined();
        expect(received?.host).toBe(rebalancerHost);

        host.unsubscribeFromReports(listener);
        host.resumeRebalancing();
        // It's actually resumed, but the listener was unsubscribed first, so it
        // never observed it.
        expect(received?.status).toBe("suspended");
        expect(host.getRebalancingReport()?.status).toBe("executing");

        // Request-Reply: a timed suspension.
        const durationMs = 5_000;
        host.suspendRebalancing(durationMs);
        let live = host.getRebalancingReport();
        expect(live?.suspensionDurationMs).not.toBeUndefined();
        expect(live?.suspensionDurationMs).toBeLessThanOrEqual(durationMs);
        expect(live?.host).toBe(rebalancerHost);

        time.advance(durationMs / 2);
        live = host.getRebalancingReport();
        expect(live?.status).toBe("suspended");
        // Must be less than the time it was told to be suspended.
        expect(live?.suspensionDurationMs).toBeLessThan(durationMs);

        time.advance(durationMs); // past the deadline: auto-resumed
        live = host.getRebalancingReport();
        expect(live?.status).toBe("executing");
        expect(live?.suspensionDurationMs).toBeUndefined();
        expect(live?.host).toBe(rebalancerHost);

        // Suspend indefinitely.
        host.suspendRebalancing();
        live = host.getRebalancingReport();
        expect(live?.status).toBe("suspended");
        expect(live?.suspensionDurationMs).not.toBeUndefined();
        expect(live?.host).toBe(rebalancerHost);
      } finally {
        await cluster.dispose();
      }
    },
  );
});
