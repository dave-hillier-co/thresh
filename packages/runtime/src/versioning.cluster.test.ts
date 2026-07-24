import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { SiloAddress } from "@tsva/core/silo-address";
import type { CompatibilityKind } from "@tsva/core/version-compatibility";
import type { VersionSelectorKind } from "@tsva/core/version-selector";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import type { Message } from "@tsva/messaging/message";
import { TestCluster } from "@tsva/testing/test-cluster";

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

const COUNTER = (key: string) => new GrainId("Counter", key);

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

async function buildCluster(opts: ClusterOptions) {
  const network = new SpyNetwork();
  const cluster = await TestCluster.start({
    clusterId: "cv",
    initialSilos: opts.versions.length,
    network,
    random: opts.random ?? (() => 0),
    configureSilo: (builder, { index }) => {
      if (opts.compatibility !== undefined || opts.selector !== undefined) {
        builder.useVersioning({
          ...(opts.compatibility !== undefined ? { compatibility: opts.compatibility } : {}),
          ...(opts.selector !== undefined ? { selector: opts.selector } : {}),
        });
      }
      const v = opts.versions[index];
      if (v !== undefined) builder.registerGrain(CounterGrain, { interfaces: [ICounterAt(v)] });
    },
  });
  return {
    cluster,
    network,
    hostsOf: (id: GrainId) => cluster.silos.filter((s) => s.host.isActive(id)),
  };
}

describe("grain-interface versioning — version-aware placement", () => {
  it("places a v2 caller's activation only on a v2-capable silo", async () => {
    const { cluster, hostsOf } = await buildCluster({
      versions: [1, 2],
      compatibility: "backwardCompatible",
    });
    try {
      // The v2 caller is silo-1; only silo-1 implements v2.
      const result = await cluster.silos[1]!.host.getGrain(ICounterAt(2), "k").increment(1);
      expect(result).toBe(1);
      expect(hostsOf(COUNTER("k"))).toEqual([cluster.silos[1]]);
    } finally {
      await cluster.dispose();
    }
  });

  it("lets a v1 caller use a v2 silo (backward compatibility)", async () => {
    const { cluster, hostsOf } = await buildCluster({
      versions: [2],
      compatibility: "backwardCompatible",
    });
    try {
      const result = await cluster.silos[0]!.host.getGrain(ICounterAt(1), "k").increment(4);
      expect(result).toBe(4);
      expect(hostsOf(COUNTER("k"))).toEqual([cluster.silos[0]]);
    } finally {
      await cluster.dispose();
    }
  });

  it("strict mode places only on the exact version when one exists", async () => {
    const { cluster, hostsOf } = await buildCluster({ versions: [1, 2], compatibility: "strict" });
    try {
      await cluster.silos[1]!.host.getGrain(ICounterAt(2), "k").increment(1);
      expect(hostsOf(COUNTER("k"))).toEqual([cluster.silos[1]]);
    } finally {
      await cluster.dispose();
    }
  });

  it("strict mode falls back to any silo (best-effort) when none is compatible", async () => {
    // Only a v1 silo is alive; a v2 caller has no strict match → place anyway.
    const { cluster, hostsOf } = await buildCluster({ versions: [1], compatibility: "strict" });
    try {
      const result = await cluster.silos[0]!.host.getGrain(ICounterAt(2), "k").increment(2);
      expect(result).toBe(2);
      expect(hostsOf(COUNTER("k"))).toEqual([cluster.silos[0]]);
    } finally {
      await cluster.dispose();
    }
  });

  it("the latest selector steers placement to the newest silo", async () => {
    const { cluster, hostsOf } = await buildCluster({
      versions: [1, 2, 3],
      selector: "latest",
      random: () => 0,
    });
    try {
      await cluster.silos[0]!.host.getGrain(ICounterAt(1), "k").increment(1);
      expect(hostsOf(COUNTER("k"))).toEqual([cluster.silos[2]]); // v3 silo
    } finally {
      await cluster.dispose();
    }
  });

  it("the minimum selector steers placement to the oldest silo", async () => {
    const { cluster, hostsOf } = await buildCluster({
      versions: [1, 2, 3],
      selector: "minimum",
      random: () => 0,
    });
    try {
      await cluster.silos[0]!.host.getGrain(ICounterAt(1), "k").increment(1);
      expect(hostsOf(COUNTER("k"))).toEqual([cluster.silos[0]]); // v1 silo
    } finally {
      await cluster.dispose();
    }
  });

  it("the all selector keeps every compatible silo as a candidate", async () => {
    // random -> index 1 of the three compatible candidates, reachable only if all are kept.
    const { cluster, hostsOf } = await buildCluster({
      versions: [1, 2, 3],
      selector: "all",
      random: () => 0.5,
    });
    try {
      await cluster.silos[0]!.host.getGrain(ICounterAt(1), "k").increment(1);
      expect(hostsOf(COUNTER("k"))).toEqual([cluster.silos[1]]); // v2 silo (middle)
    } finally {
      await cluster.dispose();
    }
  });

  it("is inert in a v1-only cluster with no policy: no manifest exchange, routing unchanged", async () => {
    const { cluster, hostsOf, network } = await buildCluster({ versions: [1, 1, 1] });
    try {
      // random -> silo-0 places the grain there; a call from silo-1 routes to it.
      const result = await cluster.silos[1]!.host.getGrain(ICounterAt(1), "shared").increment(5);
      expect(result).toBe(5);
      expect(hostsOf(COUNTER("shared"))).toEqual([cluster.silos[0]]);
      expect(network.manifestRequests).toBe(0);
    } finally {
      await cluster.dispose();
    }
  });
});
