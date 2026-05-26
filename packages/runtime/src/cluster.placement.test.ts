import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";

interface IPing extends GrainWithStringKey {
  ping(): Promise<string>;
}

// Placed only on silos advertising role "worker".
const IWorker = defineGrainInterface<IPing>("IWorker.placement");
@grain({ placement: "siloRole", role: "worker" })
class WorkerGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

// Default placement, but a metadata filter prunes to worker silos first.
const IFiltered = defineGrainInterface<IPing>("IFiltered.placement");
@grain({ placementFilters: [{ kind: "metadataMatch", match: { role: "worker" } }] })
class FilteredGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

// Lands on the least-loaded silo by activation count.
const IBalanced = defineGrainInterface<IPing>("IBalanced.placement");
@grain({ placement: "resourceOptimized" })
class BalancedGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

// preferLocal, used to seed activations on a chosen silo.
const ISeed = defineGrainInterface<IPing>("ISeed.placement");
@grain({ placement: "preferLocal" })
class SeedGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

const CLUSTER = "cp";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);

class MembershipView implements MembershipService {
  constructor(
    private readonly shared: StaticMembershipService,
    private readonly local: SiloAddress,
  ) {}
  current() {
    return this.shared.current();
  }
  updates() {
    return this.shared.updates();
  }
  localSilo() {
    return this.local;
  }
}

// silo-0 is the gateway; silo-1 and silo-2 are workers.
const roleOf = (i: number): string => (i === 0 ? "gateway" : "worker");

function buildCluster(count: number) {
  const network = new InProcessNetwork();
  const addresses = Array.from({ length: count }, (_, i) => silo(i));
  const metadataFor = (s: SiloAddress) => {
    const i = addresses.findIndex((a) => a.equals(s));
    return i >= 0 ? { role: roleOf(i) } : undefined;
  };
  const membership = new StaticMembershipService(addresses[0]!, addresses, metadataFor);

  const nodes = addresses.map((local, i) => {
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership: new MembershipView(membership, local),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0, // deterministic: pick the first candidate
      metadata: { role: roleOf(i) },
    });
    node.registerGrain(WorkerGrain, { interfaces: [IWorker] });
    node.registerGrain(FilteredGrain, { interfaces: [IFiltered] });
    node.registerGrain(BalancedGrain, { interfaces: [IBalanced] });
    node.registerGrain(SeedGrain, { interfaces: [ISeed] });
    return node;
  });

  return {
    nodes,
    hostsOf: (grainId: GrainId) => nodes.filter((n) => n.isActive(grainId)),
    start: async () => {
      for (const n of nodes) await n.start();
    },
    stop: async () => {
      for (const n of nodes) await n.stop();
    },
  };
}

describe("metadata-aware placement across a cluster", () => {
  it("places a siloRole grain only on a worker silo, regardless of caller", async () => {
    const cluster = buildCluster(3);
    await cluster.start();
    try {
      // Call from the gateway (silo-0); it must still land on a worker.
      const r = await cluster.nodes[0]!.getGrain(IWorker, "w").ping();
      expect(r).toBe("pong");
      const hosts = cluster.hostsOf(new GrainId("Worker", "w"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).toBe(cluster.nodes[1]); // random->0 over [silo-1, silo-2] => silo-1
    } finally {
      await cluster.stop();
    }
  });

  it("prunes to worker silos via a metadata filter before placing", async () => {
    const cluster = buildCluster(3);
    await cluster.start();
    try {
      await cluster.nodes[0]!.getGrain(IFiltered, "f").ping();
      const hosts = cluster.hostsOf(new GrainId("Filtered", "f"));
      expect(hosts).toHaveLength(1);
      // The gateway (silo-0) was filtered out; placement landed on a worker.
      expect(hosts[0]).not.toBe(cluster.nodes[0]);
      expect([cluster.nodes[1], cluster.nodes[2]]).toContain(hosts[0]);
    } finally {
      await cluster.stop();
    }
  });

  it("avoids a loaded silo with resource-optimized placement", async () => {
    const cluster = buildCluster(3);
    await cluster.start();
    try {
      // Seed activations on silo-0 (preferLocal from node 0) so it is the loaded silo.
      await cluster.nodes[0]!.getGrain(ISeed, "a").ping();
      await cluster.nodes[0]!.getGrain(ISeed, "b").ping();
      await cluster.nodes[0]!.getGrain(ISeed, "c").ping();

      // A resource-optimized grain placed from the loaded node avoids silo-0.
      await cluster.nodes[0]!.getGrain(IBalanced, "x").ping();
      const hosts = cluster.hostsOf(new GrainId("Balanced", "x"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).not.toBe(cluster.nodes[0]);
    } finally {
      await cluster.stop();
    }
  });
});
