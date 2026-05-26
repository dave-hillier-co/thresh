import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import type { CompatibilityKind } from "@tsva/core/version-compatibility";
import type { VersionSelectorKind } from "@tsva/core/version-selector";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import type { Message } from "@tsva/messaging/message";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { StaticMembershipService } from "@tsva/runtime/static-membership";

interface ICounter extends GrainWithStringKey {
  increment(by: number): Promise<number>;
}
// All versions share one name → one id; only the version differs.
const NAME = "ICounter.versioning";
const ICounterAt = (version: number) => defineGrainInterface<ICounter>(NAME, { version });

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

const CLUSTER = "cv";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);
const COUNTER = (key: string) => new GrainId("Counter", key);

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

/** In-process network that counts manifest-exchange requests it carries. */
class SpyNetwork extends InProcessNetwork {
  manifestRequests = 0;
  override deliver(to: SiloAddress, message: Message, from: SiloAddress): void {
    if (message.system === "manifest") this.manifestRequests++;
    super.deliver(to, message, from);
  }
}

interface ClusterOptions {
  /** Hosted interface version per silo; `undefined` means the silo hosts none. */
  versions: ReadonlyArray<number | undefined>;
  compatibility?: CompatibilityKind;
  selector?: VersionSelectorKind;
  random?: () => number;
}

function buildCluster(opts: ClusterOptions) {
  const network = new SpyNetwork();
  const addresses = opts.versions.map((_, i) => silo(i));
  const membership = new StaticMembershipService(addresses[0]!, addresses);

  const nodes = addresses.map((local, i) => {
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership: new MembershipView(membership, local),
      transport: new InProcessTransport(network, CLUSTER),
      random: opts.random ?? (() => 0),
      ...(opts.compatibility !== undefined ? { versionCompatibility: opts.compatibility } : {}),
      ...(opts.selector !== undefined ? { versionSelector: opts.selector } : {}),
    });
    const v = opts.versions[i];
    if (v !== undefined) node.registerGrain(CounterGrain, { interfaces: [ICounterAt(v)] });
    return node;
  });

  return {
    nodes,
    network,
    membership,
    hostsOf: (id: GrainId) => nodes.filter((n) => n.isActive(id)),
    start: async () => {
      for (const n of nodes) await n.start();
    },
    stop: async () => {
      for (const n of nodes) await n.stop();
    },
  };
}

describe("grain-interface versioning — version-aware placement", () => {
  it("places a v2 caller's activation only on a v2-capable silo", async () => {
    const cluster = buildCluster({ versions: [1, 2], compatibility: "backwardCompatible" });
    await cluster.start();
    try {
      // The v2 caller is silo-1; only silo-1 implements v2.
      const result = await cluster.nodes[1]!.getGrain(ICounterAt(2), "k").increment(1);
      expect(result).toBe(1);
      expect(cluster.hostsOf(COUNTER("k"))).toEqual([cluster.nodes[1]]);
    } finally {
      await cluster.stop();
    }
  });

  it("lets a v1 caller use a v2 silo (backward compatibility)", async () => {
    const cluster = buildCluster({ versions: [2], compatibility: "backwardCompatible" });
    await cluster.start();
    try {
      const result = await cluster.nodes[0]!.getGrain(ICounterAt(1), "k").increment(4);
      expect(result).toBe(4);
      expect(cluster.hostsOf(COUNTER("k"))).toEqual([cluster.nodes[0]]);
    } finally {
      await cluster.stop();
    }
  });

  it("strict mode places only on the exact version when one exists", async () => {
    const cluster = buildCluster({ versions: [1, 2], compatibility: "strict" });
    await cluster.start();
    try {
      await cluster.nodes[1]!.getGrain(ICounterAt(2), "k").increment(1);
      expect(cluster.hostsOf(COUNTER("k"))).toEqual([cluster.nodes[1]]);
    } finally {
      await cluster.stop();
    }
  });

  it("strict mode falls back to any silo (best-effort) when none is compatible", async () => {
    // Only a v1 silo is alive; a v2 caller has no strict match → place anyway.
    const cluster = buildCluster({ versions: [1], compatibility: "strict" });
    await cluster.start();
    try {
      const result = await cluster.nodes[0]!.getGrain(ICounterAt(2), "k").increment(2);
      expect(result).toBe(2);
      expect(cluster.hostsOf(COUNTER("k"))).toEqual([cluster.nodes[0]]);
    } finally {
      await cluster.stop();
    }
  });

  it("the latest selector steers placement to the newest silo", async () => {
    const cluster = buildCluster({ versions: [1, 2, 3], selector: "latest", random: () => 0 });
    await cluster.start();
    try {
      await cluster.nodes[0]!.getGrain(ICounterAt(1), "k").increment(1);
      expect(cluster.hostsOf(COUNTER("k"))).toEqual([cluster.nodes[2]]); // v3 silo
    } finally {
      await cluster.stop();
    }
  });

  it("the minimum selector steers placement to the oldest silo", async () => {
    const cluster = buildCluster({ versions: [1, 2, 3], selector: "minimum", random: () => 0 });
    await cluster.start();
    try {
      await cluster.nodes[0]!.getGrain(ICounterAt(1), "k").increment(1);
      expect(cluster.hostsOf(COUNTER("k"))).toEqual([cluster.nodes[0]]); // v1 silo
    } finally {
      await cluster.stop();
    }
  });

  it("the all selector keeps every compatible silo as a candidate", async () => {
    // random -> index 1 of the three compatible candidates, reachable only if all are kept.
    const cluster = buildCluster({ versions: [1, 2, 3], selector: "all", random: () => 0.5 });
    await cluster.start();
    try {
      await cluster.nodes[0]!.getGrain(ICounterAt(1), "k").increment(1);
      expect(cluster.hostsOf(COUNTER("k"))).toEqual([cluster.nodes[1]]); // v2 silo (middle)
    } finally {
      await cluster.stop();
    }
  });

  it("is inert in a v1-only cluster with no policy: no manifest exchange, routing unchanged", async () => {
    const cluster = buildCluster({ versions: [1, 1, 1] });
    await cluster.start();
    try {
      // random -> silo-0 places the grain there; a call from silo-1 routes to it.
      const result = await cluster.nodes[1]!.getGrain(ICounterAt(1), "shared").increment(5);
      expect(result).toBe(5);
      expect(cluster.hostsOf(COUNTER("shared"))).toEqual([cluster.nodes[0]]);
      expect(cluster.network.manifestRequests).toBe(0);
    } finally {
      await cluster.stop();
    }
  });
});
