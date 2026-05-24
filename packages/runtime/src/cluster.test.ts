import { beforeEach, describe, expect, it } from "vitest";
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

interface ICounter extends GrainWithStringKey {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.cluster", { methods: ["increment"] });

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

// preferLocal makes each node try to activate locally, forcing a CAS race.
interface ILocal extends GrainWithStringKey {
  ping(): Promise<string>;
}
const ILocal = defineGrainInterface<ILocal>("ILocal.cluster", { methods: ["ping"] });

@grain({ placement: "preferLocal" })
class LocalGrain extends Grain implements ILocal {
  async ping(): Promise<string> {
    return "pong";
  }
}

const CLUSTER = "c1";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);

/** Reports each node's own localSilo() while sharing one membership view. */
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

function buildCluster(count: number) {
  const network = new InProcessNetwork();
  const addresses = Array.from({ length: count }, (_, i) => silo(i));
  const membership = new StaticMembershipService(addresses[0]!, addresses);

  // No directoryPeer: each node routes directory operations over the transport.
  const nodes = addresses.map((local) => {
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership: new MembershipView(membership, local),
      transport: new InProcessTransport(network, CLUSTER),
      random: () => 0, // deterministic placement -> first candidate (silo-0)
    });
    node.registerGrain(CounterGrain, { interfaces: [ICounter] });
    node.registerGrain(LocalGrain, { interfaces: [ILocal] });
    return node;
  });

  return {
    nodes,
    membership,
    hostsOf: (grainId: GrainId) => nodes.filter((n) => n.isActive(grainId)),
    start: async () => {
      for (const n of nodes) await n.start();
    },
    stop: async () => {
      for (const n of nodes) await n.stop();
    },
  };
}

describe("multi-silo cluster", () => {
  beforeEach(() => undefined);

  it("routes a call from one silo to a grain placed on another, sharing its state", async () => {
    const cluster = buildCluster(3);
    await cluster.start();
    try {
      // random->0 places "Counter/shared" on silo-0; calls from other silos route there.
      const r1 = await cluster.nodes[1]!.getGrain(ICounter, "shared").increment(5);
      const r2 = await cluster.nodes[2]!.getGrain(ICounter, "shared").increment(3);
      expect(r1).toBe(5);
      expect(r2).toBe(8); // same activation, state shared across the cluster

      const hosts = cluster.hostsOf(new GrainId("Counter", "shared"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).toBe(cluster.nodes[0]); // placed on silo-0, not the callers
    } finally {
      await cluster.stop();
    }
  });

  it("activates a grain on exactly one silo regardless of which silos call it", async () => {
    const cluster = buildCluster(3);
    await cluster.start();
    try {
      await Promise.all(cluster.nodes.map((n) => n.getGrain(ICounter, "one").increment(1)));
      expect(cluster.hostsOf(new GrainId("Counter", "one"))).toHaveLength(1);
    } finally {
      await cluster.stop();
    }
  });

  it("resolves a forced activation race to a single winner via directory CAS", async () => {
    const cluster = buildCluster(3);
    await cluster.start();
    try {
      const results = await Promise.all(
        cluster.nodes.map((n) => n.getGrain(ILocal, "race").ping()),
      );
      expect(results).toEqual(["pong", "pong", "pong"]);
      expect(cluster.hostsOf(new GrainId("Local", "race"))).toHaveLength(1);
    } finally {
      await cluster.stop();
    }
  });

  it("rebalances when a silo leaves: the grain reactivates elsewhere on next call", async () => {
    const cluster = buildCluster(2);
    await cluster.start();
    const grainId = new GrainId("Counter", "moves");
    try {
      // random->0 places it on silo-0; build up some state.
      expect(await cluster.nodes[1]!.getGrain(ICounter, "moves").increment(5)).toBe(5);
      expect(cluster.hostsOf(grainId)).toEqual([cluster.nodes[0]]);

      // silo-0 leaves the cluster.
      cluster.membership.removeSilo(silo(0));
      await cluster.nodes[0]!.stop();
      cluster.nodes[1]!.updateView();

      // Next call reactivates the grain on the surviving silo, with fresh state.
      expect(await cluster.nodes[1]!.getGrain(ICounter, "moves").increment(7)).toBe(7);
      expect(cluster.hostsOf(grainId)).toEqual([cluster.nodes[1]]);
    } finally {
      await cluster.nodes[1]!.stop();
    }
  });
});
