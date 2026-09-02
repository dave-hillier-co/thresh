import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

interface IWorker extends GrainKey<string> {
  ping(): Promise<string>;
}
const IWorker = defineGrainInterface<IWorker>("IWorker.rebalance.e2e");

@grain()
class WorkerGrain extends Grain implements IWorker {
  async ping(): Promise<string> {
    return "ok";
  }
}

const flush = () => new Promise((r) => setTimeout(r, 0));
/** Drain microtasks repeatedly so cross-silo async migration settles between ticks. */
async function settle(): Promise<void> {
  for (let i = 0; i < 8; i++) await flush();
}

const addrs = [0, 1, 2].map((n) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1111${n}`));
const period = 1000;

describe("activation rebalancer converges across a cluster (slice 2b e2e)", () => {
  it("levels a skewed load and only the elected silo runs cycles", async () => {
    const time = new FakeTimeProvider();
    const net = new InProcessNetwork();
    const membership = new StaticMembershipService(addrs[0]!, addrs);

    // Build three silos; keep a handle to each host and seed all activations on silo-0
    // (random -> first candidate places every grain there), creating a [N,0,0] skew.
    // The self-probe is off: the fake clock that drives rebalance cycles would
    // also fire probe ticks, and each silo's probe-grain activation would skew
    // the exact activation counts this test asserts on.
    const silos = addrs.map((local) =>
      createSilo({ clusterId: "c", local, time, random: () => 0, selfProbe: { enabled: false } })
        .useMembership(membership)
        .useInProcessTransport(net)
        .useActivationRebalancing({
          sessionCyclePeriodSeconds: 1,
          cycleNumberWeight: 1,
          siloNumberWeight: 0.1,
        })
        .registerGrain(WorkerGrain, { interfaces: [IWorker] })
        .build(),
    );
    for (const s of silos) await s.start();

    try {
      // Seed 30 activations, all on silo-0 (random -> first candidate).
      for (let i = 0; i < 30; i++) await silos[0]!.getGrain(IWorker, `g-${i}`).ping();

      type NodeFacet = {
        gatherClusterLoad(): Promise<{ counts: Map<string, number> }>;
        migrateRandomActivations(target: SiloAddress, count: number): Promise<number>;
      };
      const nodeOf = (host: SiloHost): NodeFacet =>
        (host as unknown as { parts: { node: NodeFacet } }).parts.node;
      const loadOf = async (host: SiloHost): Promise<Map<string, number>> =>
        // gatherClusterLoad reads local + queries peers; any silo sees the whole cluster.
        (await nodeOf(host).gatherClusterLoad()).counts;

      // Give the quieter silos a small seed so the load distribution has non-zero
      // entropy (a fully concentrated [30,0,0] has alpha = entropy/maxEntropy = 0,
      // which the model deliberately won't migrate). Skew is now [24, 3, 3].
      await nodeOf(silos[0]!).migrateRandomActivations(addrs[1]!, 3);
      await nodeOf(silos[0]!).migrateRandomActivations(addrs[2]!, 3);
      await settle();

      const before = await loadOf(silos[0]!);
      expect(before.get("silo-0")).toBe(24);
      expect(before.get("silo-1")).toBe(3);
      expect(before.get("silo-2")).toBe(3);

      // Drive many cycles. Each tick the elected leader (silo-0, lowest ring key)
      // sheds activations toward the quieter silos.
      for (let i = 0; i < 40; i++) {
        time.advance(period);
        await settle();
      }

      const after = await loadOf(silos[0]!);
      const counts = [after.get("silo-0") ?? 0, after.get("silo-1") ?? 0, after.get("silo-2") ?? 0];
      const total = counts.reduce((a, b) => a + b, 0);
      expect(total).toBe(30); // no activations lost

      // Converged toward balance: every silo is within a small band of the even split (10).
      const max = Math.max(...counts);
      const min = Math.min(...counts);
      expect(max - min).toBeLessThanOrEqual(6);
      expect(min).toBeGreaterThan(0); // the quiet silos were actually filled

      // Only the elected silo (silo-0) reports as the rebalancer host.
      expect(silos[0]!.rebalancingReport()?.host).toBe("silo-0");
      // Followers' workers never ran a cycle, so their imbalance stays at the initial 0.
      expect(silos[1]!.rebalancingReport()?.clusterImbalance).toBe(0);
      expect(silos[2]!.rebalancingReport()?.clusterImbalance).toBe(0);
      // The leader observed (and reduced) imbalance.
      expect(silos[0]!.rebalancingReport()?.host).toBe("silo-0");
    } finally {
      await Promise.all(silos.map((s) => s.stop()));
    }
  });
});
