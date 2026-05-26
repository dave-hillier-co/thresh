import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type {
  DehydrationContext,
  IGrainMigrationParticipant,
  RehydrationContext,
} from "@tsva/core/grain-migration-participant";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { MembershipService } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork, InProcessTransport } from "@tsva/messaging/in-process-transport";
import { ClusterNode } from "@tsva/runtime/cluster-node";
import { Silo } from "@tsva/runtime/silo";
import { StaticMembershipService } from "@tsva/runtime/static-membership";
import { FakeTimeProvider } from "@tsva/runtime/test-support/fake-time-provider";

interface IMigratingCounter extends GrainWithStringKey {
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

const CLUSTER = "c-migration";
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);
const grainId = new GrainId("MigratingCounter", "x");

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

function buildCluster(count: number, time: FakeTimeProvider) {
  const network = new InProcessNetwork();
  const addresses = Array.from({ length: count }, (_, i) => silo(i));
  const membership = new StaticMembershipService(addresses[0]!, addresses);
  const nodes = addresses.map((local) => {
    const node = new ClusterNode({
      local,
      clusterId: CLUSTER,
      membership: new MembershipView(membership, local),
      transport: new InProcessTransport(network, CLUSTER),
      time,
      defaultCollectionAgeSeconds: 30,
      collectionIntervalSeconds: 10,
      random: () => 0, // deterministic placement -> first candidate (silo-0)
    });
    node.registerGrain(MigratingCounterGrain, { interfaces: [IMigratingCounter] });
    return node;
  });
  return {
    nodes,
    hostsOf: () => nodes.filter((n) => n.isActive(grainId)),
    start: async () => {
      for (const n of nodes) await n.start();
    },
    stop: async () => {
      for (const n of nodes) await n.stop();
    },
  };
}

const flush = () => new Promise((r) => setTimeout(r, 0));
async function settleUntil(pred: () => boolean, max = 200): Promise<void> {
  for (let i = 0; i < max && !pred(); i++) await flush();
  await flush();
}

describe("grain migration (multi-silo)", () => {
  it("migrates an idle activation to a directed silo, preserving its state", async () => {
    const time = new FakeTimeProvider();
    const cluster = buildCluster(3, time);
    await cluster.start();
    try {
      // random->0 places the grain on silo-0; mutate its in-memory state there.
      expect(await cluster.nodes[1]!.getGrain(IMigratingCounter, "x").increment(5)).toBe(5);
      expect(cluster.hostsOf()).toEqual([cluster.nodes[0]]);

      // Ask it to migrate to silo-1 once idle.
      await cluster.nodes[1]!.getGrain(IMigratingCounter, "x").scheduleMigration(silo(1));

      // The collector sweep past the collection age migrates rather than collects.
      time.advance(31_000);
      await settleUntil(() => cluster.nodes[1]!.isActive(grainId));

      expect(cluster.nodes[0]!.isActive(grainId)).toBe(false);
      expect(cluster.hostsOf()).toEqual([cluster.nodes[1]]);

      // State survived the move: the new host reports the accumulated count.
      expect(await cluster.nodes[2]!.getGrain(IMigratingCounter, "x").get()).toBe(5);
    } finally {
      await cluster.stop();
    }
  });

  it("migrates to a strategy-chosen silo (other than the current host) when undirected", async () => {
    const time = new FakeTimeProvider();
    const cluster = buildCluster(3, time);
    await cluster.start();
    try {
      await cluster.nodes[1]!.getGrain(IMigratingCounter, "x").increment(8);
      expect(cluster.hostsOf()).toEqual([cluster.nodes[0]]);

      // No target: placement picks among the *other* silos (random->0 => silo-1).
      await cluster.nodes[1]!.getGrain(IMigratingCounter, "x").scheduleMigration();

      time.advance(31_000);
      await settleUntil(
        () => !cluster.nodes[0]!.isActive(grainId) && cluster.hostsOf().length === 1,
      );

      const hosts = cluster.hostsOf();
      expect(hosts).toHaveLength(1);
      expect(hosts[0]).not.toBe(cluster.nodes[0]);
      expect(await hosts[0]!.getGrain(IMigratingCounter, "x").get()).toBe(8);
    } finally {
      await cluster.stop();
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
