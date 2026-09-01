import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import type { PlacementStrategy } from "@thresh/runtime/placement/placement-strategy";
import type { GrainKey } from "@thresh/core/key-kinds";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

interface IPing extends GrainKey<string> {
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

// silo-0 is the gateway; silo-1 and silo-2 are workers.
const roleOf = (i: number): string => (i === 0 ? "gateway" : "worker");

function buildCluster(count: number) {
  return TestCluster.start({
    clusterId: "cp",
    initialSilos: count,
    random: () => 0, // deterministic: pick the first candidate
    siloMetadata: ({ index }) => ({ role: roleOf(index) }),
    grains: [
      { ctor: WorkerGrain, interfaces: [IWorker] },
      { ctor: FilteredGrain, interfaces: [IFiltered] },
      { ctor: BalancedGrain, interfaces: [IBalanced] },
      { ctor: SeedGrain, interfaces: [ISeed] },
    ],
  });
}

const hostsOf = (cluster: TestCluster, grainId: GrainId): ReadonlyArray<TestSiloHandle> =>
  cluster.silos.filter((s) => s.host.isActive(grainId));

describe("metadata-aware placement across a cluster", () => {
  it("places a siloRole grain only on a worker silo, regardless of caller", async () => {
    const cluster = await buildCluster(3);
    try {
      // Call from the gateway (silo-0); it must still land on a worker.
      const r = await cluster.silos[0]!.host.getGrain(IWorker, "w").ping();
      expect(r).toBe("pong");
      const hosts = hostsOf(cluster, new GrainId("Worker", "w"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).toBe(cluster.silos[1]); // random->0 over [silo-1, silo-2] => silo-1
    } finally {
      await cluster.dispose();
    }
  });

  it("prunes to worker silos via a metadata filter before placing", async () => {
    const cluster = await buildCluster(3);
    try {
      await cluster.silos[0]!.host.getGrain(IFiltered, "f").ping();
      const hosts = hostsOf(cluster, new GrainId("Filtered", "f"));
      expect(hosts).toHaveLength(1);
      // The gateway (silo-0) was filtered out; placement landed on a worker.
      expect(hosts[0]).not.toBe(cluster.silos[0]);
      expect([cluster.silos[1], cluster.silos[2]]).toContain(hosts[0]);
    } finally {
      await cluster.dispose();
    }
  });

  it("avoids a loaded silo with resource-optimized placement", async () => {
    const cluster = await buildCluster(3);
    try {
      // Seed activations on silo-0 (preferLocal from node 0) so it is the loaded silo.
      await cluster.silos[0]!.host.getGrain(ISeed, "a").ping();
      await cluster.silos[0]!.host.getGrain(ISeed, "b").ping();
      await cluster.silos[0]!.host.getGrain(ISeed, "c").ping();

      // A resource-optimized grain placed from the loaded node avoids silo-0.
      await cluster.silos[0]!.host.getGrain(IBalanced, "x").ping();
      const hosts = hostsOf(cluster, new GrainId("Balanced", "x"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).not.toBe(cluster.silos[0]);
    } finally {
      await cluster.dispose();
    }
  });
});

// No `placement` in its metadata: this grain type is the one a silo-wide
// default strategy is allowed to steer (Orleans' `PlacementStrategy` DI
// singleton, which `PlacementStrategyResolver` falls back to).
const IUnplaced = defineGrainInterface<IPing>("IUnplaced.placement");
@grain()
class UnplacedGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

// Explicitly random: an explicit per-class choice must beat the silo default.
const IExplicitRandom = defineGrainInterface<IPing>("IExplicitRandom.placement");
@grain({ placement: "random" })
class ExplicitRandomGrain extends Grain implements IPing {
  async ping(): Promise<string> {
    return "pong";
  }
}

function buildDefaultOverrideCluster(strategy?: PlacementStrategy) {
  return TestCluster.start({
    clusterId: "cp-default",
    initialSilos: 3,
    random: () => 0, // deterministic: RandomPlacement picks silo-0
    ...(strategy !== undefined ? { defaultPlacementStrategy: strategy } : {}),
    grains: [
      { ctor: UnplacedGrain, interfaces: [IUnplaced] },
      { ctor: ExplicitRandomGrain, interfaces: [IExplicitRandom] },
    ],
  });
}

describe("silo-wide default placement strategy", () => {
  it("places a grain that declares no placement with RandomPlacement when unset", async () => {
    const cluster = await buildDefaultOverrideCluster();
    try {
      await cluster.silos[2]!.host.getGrain(IUnplaced, "u").ping();
      const hosts = hostsOf(cluster, new GrainId("Unplaced", "u"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).toBe(cluster.silos[0]);
    } finally {
      await cluster.dispose();
    }
  });

  it("steers a grain that declares no placement", async () => {
    const cluster = await buildDefaultOverrideCluster(new PreferLocalPlacement());
    try {
      // Called from silo-2: preferLocal keeps it there, where random->0 would
      // have sent it to silo-0.
      await cluster.silos[2]!.host.getGrain(IUnplaced, "u").ping();
      const hosts = hostsOf(cluster, new GrainId("Unplaced", "u"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).toBe(cluster.silos[2]);
    } finally {
      await cluster.dispose();
    }
  });

  it("does not override an explicit per-class placement", async () => {
    const cluster = await buildDefaultOverrideCluster(new PreferLocalPlacement());
    try {
      await cluster.silos[2]!.host.getGrain(IExplicitRandom, "r").ping();
      const hosts = hostsOf(cluster, new GrainId("ExplicitRandom", "r"));
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).toBe(cluster.silos[0]);
    } finally {
      await cluster.dispose();
    }
  });
});
