import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type {
  DehydrationContext,
  IGrainMigrationParticipant,
  RehydrationContext,
} from "@thresh/core/grain-migration-participant";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { SiloAddress } from "@thresh/core/silo-address";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { TestCluster } from "@thresh/testing/test-cluster";

interface IMigratingCounter extends GrainKey<string> {
  increment(by: number): Promise<number>;
  get(): Promise<number>;
  scheduleMigration(target?: SiloAddress): Promise<void>;
}
const IMigratingCounter = defineGrainInterface<IMigratingCounter>("IMigratingCounter.migration", {
  options: { get: { readOnly: true } },
});

@grain()
class MigratingCounterGrain extends Grain implements IMigratingCounter, IGrainMigrationParticipant {
  private count = 0;

  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }

  async get(): Promise<number> {
    return this.count;
  }

  async scheduleMigration(target?: SiloAddress): Promise<void> {
    this.runtime.migrateOnIdle(target);
  }

  onDehydrate(ctx: DehydrationContext): void {
    ctx.set("count", this.count);
  }

  onRehydrate(ctx: RehydrationContext): void {
    const c = ctx.get<number>("count");
    if (c !== undefined) this.count = c;
  }
}

const grainId = new GrainId("MigratingCounter", "x");

function buildCluster(count: number, time: FakeTimeProvider) {
  return TestCluster.start({
    clusterId: "c-migration",
    initialSilos: count,
    time,
    collectionAgeSeconds: 30,
    collectionIntervalSeconds: 10,
    random: () => 0, // deterministic placement -> first candidate (silo-0)
    grains: [{ ctor: MigratingCounterGrain, interfaces: [IMigratingCounter] }],
  });
}

const flush = () => new Promise((r) => setTimeout(r, 0));
async function settleUntil(pred: () => boolean, max = 200): Promise<void> {
  for (let i = 0; i < max && !pred(); i++) await flush();
  await flush();
}

describe("grain migration (multi-silo)", () => {
  it("migrates an idle activation to a directed silo, preserving its state", async () => {
    const time = new FakeTimeProvider();
    const cluster = await buildCluster(3, time);
    const hostsOf = () => cluster.silos.filter((s) => s.host.isActive(grainId));
    try {
      // random->0 places the grain on silo-0; mutate its in-memory state there.
      expect(await cluster.silos[1]!.host.getGrain(IMigratingCounter, "x").increment(5)).toBe(5);
      expect(hostsOf()).toEqual([cluster.silos[0]]);

      // Ask it to migrate to silo-1 once idle.
      await cluster.silos[1]!.host.getGrain(IMigratingCounter, "x").scheduleMigration(
        cluster.silos[1]!.address,
      );

      // The collector sweep past the collection age migrates rather than collects.
      time.advance(31_000);
      await settleUntil(() => cluster.silos[1]!.host.isActive(grainId));

      expect(cluster.silos[0]!.host.isActive(grainId)).toBe(false);
      expect(hostsOf()).toEqual([cluster.silos[1]]);

      // State survived the move: the new host reports the accumulated count.
      expect(await cluster.silos[2]!.host.getGrain(IMigratingCounter, "x").get()).toBe(5);
    } finally {
      await cluster.dispose();
    }
  });

  it("migrates to a strategy-chosen silo (other than the current host) when undirected", async () => {
    const time = new FakeTimeProvider();
    const cluster = await buildCluster(3, time);
    const hostsOf = () => cluster.silos.filter((s) => s.host.isActive(grainId));
    try {
      await cluster.silos[1]!.host.getGrain(IMigratingCounter, "x").increment(8);
      expect(hostsOf()).toEqual([cluster.silos[0]]);

      // No target: placement picks among the *other* silos (random->0 => silo-1).
      await cluster.silos[1]!.host.getGrain(IMigratingCounter, "x").scheduleMigration();

      time.advance(31_000);
      await settleUntil(() => !cluster.silos[0]!.host.isActive(grainId) && hostsOf().length === 1);

      const hosts = hostsOf();
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).not.toBe(cluster.silos[0]);
      expect(await hosts[0]!.host.getGrain(IMigratingCounter, "x").get()).toBe(8);
    } finally {
      await cluster.dispose();
    }
  });

  it("falls back to plain idle deactivation on a single silo (nowhere to migrate)", async () => {
    const time = new FakeTimeProvider();
    const host = new Silo({ time, defaultCollectionAgeSeconds: 30, collectionIntervalSeconds: 10 });
    host.registerGrain(MigratingCounterGrain, { interfaces: [IMigratingCounter] });
    host.start();
    try {
      const counter = host.getGrain(IMigratingCounter, "y");
      await counter.increment(3);
      await counter.scheduleMigration();
      const id = new GrainId("MigratingCounter", "y");
      expect(host.isActive(id)).toBe(true);

      time.advance(31_000);
      await flush();

      // No migrate hook on a single silo: it simply deactivates and reactivates fresh.
      expect(host.isActive(id)).toBe(false);
      expect(await counter.get()).toBe(0);
    } finally {
      await host.stop();
    }
  });
});
