import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { TestCluster } from "@thresh/testing/test-cluster";

interface IMovable extends GrainKey<string> {
  ping(): Promise<string>;
  migrateWhenIdle(): Promise<void>;
}

const movable = (name: string) => defineGrainInterface<IMovable>(name);

// Pinned to silos with role "worker"; seeded on silo-1.
const IRoleMovable = movable("IRoleMovable.migplace");
@grain({ placement: "siloRole", role: "worker" })
class RoleMovableGrain extends Grain implements IMovable {
  async ping(): Promise<string> {
    return "pong";
  }
  async migrateWhenIdle(): Promise<void> {
    this.runtime.migrateOnIdle();
  }
}

// Filtered to zone "a".
const IZoned = movable("IZoned.migplace");
@grain({ placementFilters: [{ kind: "metadataMatch", match: { zone: "a" } }] })
class ZonedGrain extends Grain implements IMovable {
  async ping(): Promise<string> {
    return "pong";
  }
  async migrateWhenIdle(): Promise<void> {
    this.runtime.migrateOnIdle();
  }
}

// Load-aware placement.
const ICounted = movable("ICounted.migplace");
@grain({ placement: "activationCount" })
class CountedGrain extends Grain implements IMovable {
  async ping(): Promise<string> {
    return "pong";
  }
  async migrateWhenIdle(): Promise<void> {
    this.runtime.migrateOnIdle();
  }
}

const ISeed = movable("ISeed.migplace");
@grain({ placement: "preferLocal" })
class SeedGrain extends Grain implements IMovable {
  async ping(): Promise<string> {
    return "pong";
  }
  async migrateWhenIdle(): Promise<void> {
    this.runtime.migrateOnIdle();
  }
}

const IWorker = movable("IWorker.migplace");
@grain({ stateless: true })
class StatelessWorkerGrain extends Grain implements IMovable {
  async ping(): Promise<string> {
    return "pong";
  }
  async migrateWhenIdle(): Promise<void> {}
}

const flush = () => new Promise((r) => setTimeout(r, 0));
async function settleUntil(pred: () => boolean, max = 300): Promise<void> {
  for (let i = 0; i < max && !pred(); i++) await flush();
  await flush();
}

async function build(time: FakeTimeProvider, metadata: (i: number) => Record<string, string>) {
  return TestCluster.start({
    clusterId: "c-migplace",
    initialSilos: 3,
    time,
    collectionAgeSeconds: 30,
    collectionIntervalSeconds: 10,
    random: () => 0,
    loadPublishIntervalMs: 10,
    siloMetadata: ({ index }) => metadata(index),
    grains: [
      { ctor: RoleMovableGrain, interfaces: [IRoleMovable] },
      { ctor: ZonedGrain, interfaces: [IZoned] },
      { ctor: CountedGrain, interfaces: [ICounted] },
      { ctor: SeedGrain, interfaces: [ISeed] },
    ],
  });
}

describe("migration goes through the shared placement pipeline", () => {
  it("migrates a role-pinned grain on idle to another silo of that role", async () => {
    const time = new FakeTimeProvider();
    // silo-0 gateway, silo-1 and silo-2 workers.
    const cluster = await build(time, (i) => ({ role: i === 0 ? "gateway" : "worker" }));
    try {
      const id = new GrainId("RoleMovable", "r");
      await cluster.silos[1]!.host.getGrain(IRoleMovable, "r").ping();
      const first = cluster.silos.find((s) => s.host.isActive(id))!;
      expect(first).toBe(cluster.silos[1]); // random->0 over workers [silo-1, silo-2]
      await first.host.getGrain(IRoleMovable, "r").migrateWhenIdle();
      time.advance(31_000);
      await settleUntil(() => !first.host.isActive(id));
      await settleUntil(() => cluster.silos.some((s) => s.host.isActive(id)));
      const hosts = cluster.silos.filter((s) => s.host.isActive(id));
      expect(hosts).toEqual([cluster.silos[2]]);
    } finally {
      await cluster.dispose();
    }
  });

  it("honours a zone constraint when migrating on idle", async () => {
    const time = new FakeTimeProvider();
    // silo-0 and silo-2 are zone a; silo-1 is zone b.
    const cluster = await build(time, (i) => ({ zone: i === 1 ? "b" : "a" }));
    try {
      const id = new GrainId("Zoned", "z");
      await cluster.silos[0]!.host.getGrain(IZoned, "z").ping();
      expect(cluster.silos[0]!.host.isActive(id)).toBe(true);
      await cluster.silos[0]!.host.getGrain(IZoned, "z").migrateWhenIdle();
      time.advance(31_000);
      await settleUntil(() => !cluster.silos[0]!.host.isActive(id));
      await settleUntil(() => cluster.silos.some((s) => s.host.isActive(id)));
      expect(cluster.silos.filter((s) => s.host.isActive(id))).toEqual([cluster.silos[2]]);
    } finally {
      await cluster.dispose();
    }
  });

  it("lets a load-aware strategy see real peer counts when choosing a migration target", async () => {
    const time = new FakeTimeProvider();
    const cluster = await build(time, () => ({}));
    try {
      // The migrating grain starts on silo-0 (all silos empty, random->0), then silo-1
      // takes on load while silo-2 stays empty.
      const id = new GrainId("Counted", "c");
      await cluster.silos[0]!.host.getGrain(ICounted, "c").ping();
      expect(cluster.silos[0]!.host.isActive(id)).toBe(true);
      for (let i = 0; i < 6; i += 1) await cluster.silos[1]!.host.getGrain(ISeed, `s${i}`).ping();
      await cluster.silos[1]!.host.loadShedding.unlatchCpuUsage();
      await cluster.silos[2]!.host.loadShedding.unlatchCpuUsage();
      await cluster.silos[0]!.host.getGrain(ICounted, "c").migrateWhenIdle();
      time.advance(31_000);
      await settleUntil(() => !cluster.silos[0]!.host.isActive(id));
      await settleUntil(() => cluster.silos.some((s) => s.host.isActive(id)));
      expect(cluster.silos.filter((s) => s.host.isActive(id))).toEqual([cluster.silos[2]]);
    } finally {
      await cluster.dispose();
    }
  });

  it("does not obey a directed move to a silo the grain's filters exclude", async () => {
    const time = new FakeTimeProvider();
    const cluster = await build(time, (i) => ({ zone: i === 2 ? "b" : "a" }));
    try {
      const id = new GrainId("Zoned", "d");
      await cluster.silos[0]!.host.getGrain(IZoned, "d").ping();
      const moved = await cluster.silos[0]!.host.migrate(id, cluster.silos[2]!.address);
      expect(moved).toBe(true);
      expect(cluster.silos[2]!.host.isActive(id)).toBe(false);
      // Zone "a" is silo-0 and silo-1; silo-0 is excluded as the source, so placement chose silo-1.
      await settleUntil(() => cluster.silos[1]!.host.isActive(id));
      expect(cluster.silos[1]!.host.isActive(id)).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });

  it("lands a directed move on the hinted silo", async () => {
    const time = new FakeTimeProvider();
    const cluster = await build(time, () => ({}));
    try {
      const id = new GrainId("Seed", "h");
      await cluster.silos[0]!.host.getGrain(ISeed, "h").ping();
      expect(await cluster.silos[0]!.host.migrate(id, cluster.silos[2]!.address)).toBe(true);
      await settleUntil(() => cluster.silos[2]!.host.isActive(id));
      expect(cluster.silos[2]!.host.isActive(id)).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });
});

describe("migratability", () => {
  it("never moves a stateless worker with the rebalancer", async () => {
    const network = new InProcessNetwork();
    const addresses = [0, 1].map((n) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`));
    const nodes = addresses.map(
      (local) =>
        new ClusterNode({
          local,
          clusterId: "c-stateless",
          membership: new StaticMembershipService(local, addresses),
          transport: new InProcessTransport(network, "c-stateless"),
          random: () => 0,
        }),
    );
    for (const node of nodes) node.registerGrain(StatelessWorkerGrain, { interfaces: [IWorker] });
    await Promise.all(nodes.map((n) => n.start()));
    try {
      for (let i = 0; i < 4; i += 1) await nodes[0]!.getGrain(IWorker, `w${i}`).ping();
      const before = nodes[0]!.activationCount();
      expect(before).toBeGreaterThan(0);
      expect(await nodes[0]!.migrateRandomActivations(addresses[1]!, 10)).toBe(0);
      expect(nodes[0]!.activationCount()).toBe(before);
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });
});
