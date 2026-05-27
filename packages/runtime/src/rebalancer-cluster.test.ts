import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { DEFAULT_REBALANCER_OPTIONS } from "@tsva/runtime/placement/rebalancing/rebalancer-model";

interface IWorker extends GrainWithStringKey {
  ping(): Promise<string>;
}
const IWorker = defineGrainInterface<IWorker>("IWorker.rebalance");

@grain()
class WorkerGrain extends Grain implements IWorker {
  async ping(): Promise<string> {
    return "ok";
  }
}

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1111${n}`);

/** `count` in-process silos sharing one membership view and network; placement is deterministic (silo-0). */
function buildCluster(count: number): { nodes: ClusterNode[]; addresses: SiloAddress[] } {
  const network = new InProcessNetwork();
  const addresses = Array.from({ length: count }, (_, i) => silo(i));
  const nodes = addresses.map(
    (local) =>
      new ClusterNode({
        local,
        clusterId: "c1",
        membership: new StaticMembershipService(local, addresses),
        transport: new InProcessTransport(network, "c1"),
        random: () => 0, // place every grain on silo-0 (first ring candidate)
      }),
  );
  for (const node of nodes) node.registerGrain(WorkerGrain, { interfaces: [IWorker] });
  return { nodes, addresses };
}

async function activateOn(node: ClusterNode, keys: string[]): Promise<void> {
  for (const key of keys) await node.getGrain(IWorker, key).ping();
}

const keys = (prefix: string, n: number) => Array.from({ length: n }, (_, i) => `${prefix}-${i}`);

describe("activation rebalancer — load gathering + directed migration (slice 2a)", () => {
  it("gathers per-silo activation counts across the cluster", async () => {
    const { nodes } = buildCluster(2);
    await Promise.all(nodes.map((n) => n.start()));
    try {
      await activateOn(nodes[0]!, keys("g", 3)); // all land on silo-0
      const { counts } = await nodes[0]!.gatherClusterLoad();
      expect(counts.get("silo-0")).toBe(3);
      expect(counts.get("silo-1")).toBe(0);
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });

  it("migrates a requested number of random activations to a target silo", async () => {
    const { nodes, addresses } = buildCluster(2);
    await Promise.all(nodes.map((n) => n.start()));
    try {
      await activateOn(nodes[0]!, keys("g", 4));
      expect(nodes[0]!.activationCount()).toBe(4);

      const moved = await nodes[0]!.migrateRandomActivations(addresses[1]!, 2);
      expect(moved).toBe(2);
      expect(nodes[0]!.activationCount()).toBe(2);
      expect(nodes[1]!.activationCount()).toBe(2);
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });

  it("a rebalance cycle sheds activations from the busy silo toward the quiet one", async () => {
    const { nodes, addresses } = buildCluster(2);
    await Promise.all(nodes.map((n) => n.start()));
    try {
      // Seed a moderate skew: 12 activations on silo-0, then move 2 to silo-1 -> [10, 2].
      await activateOn(nodes[0]!, keys("g", 12));
      await nodes[0]!.migrateRandomActivations(addresses[1]!, 2);
      expect(nodes[0]!.activationCount()).toBe(10);
      expect(nodes[1]!.activationCount()).toBe(2);

      // Run a cycle with the session well underway (high cycle => adaptive scaling
      // near its ceiling), so the migration delta is a deterministic, non-zero count.
      const result = await nodes[0]!.runRebalanceCycle(
        { rebalancingCycle: 20, stagnantCycles: 0, previousEntropy: 0 },
        DEFAULT_REBALANCER_OPTIONS,
      );

      expect(result.moved).toBeGreaterThan(0);
      expect(result.imbalance).toBeGreaterThan(0.3); // [10,2] is clearly imbalanced
      // The busy silo shed exactly `moved` activations to the quiet one.
      expect(nodes[0]!.activationCount()).toBe(10 - result.moved);
      expect(nodes[1]!.activationCount()).toBe(2 + result.moved);
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });
});
