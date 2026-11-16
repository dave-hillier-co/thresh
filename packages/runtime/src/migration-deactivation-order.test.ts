import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type {
  DehydrationContext,
  RehydrationContext,
} from "@thresh/core/grain-migration-participant";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { InProcessNetwork, InProcessTransport } from "@thresh/messaging/in-process-transport";
import { ClusterNode } from "@thresh/runtime/cluster-node";
import { StaticMembershipService } from "@thresh/runtime/static-membership";

// L1: the rebalancer and an on-request migration (`TestCluster.migrate`'s
// path) run the SAME pipeline as idle migration, in Orleans'
// `FinishDeactivating` order: timers disposed, then `onDeactivate`, then the
// dehydrate and hand-off — never the hook after the state has shipped.

interface IOrdered extends GrainKey<string> {
  read(): Promise<string>;
}
const IOrdered = defineGrainInterface<IOrdered>("IOrdered.migration-order");

const events = {
  /** Timer ticks that ran on an activation whose state had already shipped. */
  ticksAfterShipped: 0,
  /** Order of the hook and the dehydrate on the source. */
  order: [] as string[],
};

@grain()
class OrderedGrain extends Grain implements IOrdered {
  value = "initial";
  shipped = false;

  override async onActivate(): Promise<void> {
    this.runtime.registerTimer(
      async () => {
        if (this.shipped) events.ticksAfterShipped++;
      },
      { ms: 1 },
      { ms: 1 },
    );
  }

  override async onDeactivate(): Promise<void> {
    events.order.push("onDeactivate");
    this.value = "set-in-onDeactivate";
    // Give an armed timer every chance to tick across the hand-off.
    await new Promise((r) => setTimeout(r, 5));
  }

  onDehydrate(ctx: DehydrationContext): void {
    events.order.push("dehydrate");
    ctx.set("value", this.value);
    this.shipped = true;
  }

  onRehydrate(ctx: RehydrationContext): void {
    const v = ctx.get<string>("value");
    if (v !== undefined) this.value = v;
  }

  async read(): Promise<string> {
    return this.value;
  }
}

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1113${n}`);

function buildCluster(): {
  nodes: ClusterNode[];
  addresses: SiloAddress[];
  unbound: Array<{ silo: number; grainId: string }>;
} {
  const network = new InProcessNetwork();
  const addresses = [silo(0), silo(1)];
  const unbound: Array<{ silo: number; grainId: string }> = [];
  const nodes = addresses.map(
    (local, index) =>
      new ClusterNode({
        local,
        clusterId: "c1",
        membership: new StaticMembershipService(local, addresses),
        transport: new InProcessTransport(network, "c1"),
        random: () => 0, // place every grain on silo-0
        stateUnbinder: (_instance, grainId) =>
          unbound.push({ silo: index, grainId: grainId.toString() }),
      }),
  );
  for (const node of nodes) node.registerGrain(OrderedGrain, { interfaces: [IOrdered] });
  return { nodes, addresses, unbound };
}

function reset(): void {
  events.ticksAfterShipped = 0;
  events.order = [];
}

describe("migration through the rebalancer or on request runs onDeactivate before the dehydrate (L1)", () => {
  it("rebalancer: state changed in onDeactivate arrives on the target and no timer ticks after the state shipped", async () => {
    reset();
    const { nodes, addresses, unbound } = buildCluster();
    await Promise.all(nodes.map((n) => n.start()));
    try {
      const ref = nodes[0]!.getGrain(IOrdered, "r1");
      expect(await ref.read()).toBe("initial");
      await new Promise((r) => setTimeout(r, 5)); // let the timer tick a few times

      const moved = await nodes[0]!.migrateRandomActivations(addresses[1]!, 1);
      expect(moved).toBe(1);

      expect(events.order).toEqual(["onDeactivate", "dehydrate"]);
      expect(await ref.read()).toBe("set-in-onDeactivate");
      await new Promise((r) => setTimeout(r, 5));
      expect(events.ticksAfterShipped).toBe(0);
      // The source finished its deactivation: unbound once, nothing left behind.
      expect(unbound.filter((u) => u.silo === 0)).toHaveLength(1);
      expect(nodes[0]!.activationCount()).toBe(0);
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });

  it("on request (TestCluster.migrate's path): onDeactivate runs before the dehydrate and the source is finalized once", async () => {
    reset();
    const { nodes, addresses, unbound } = buildCluster();
    await Promise.all(nodes.map((n) => n.start()));
    try {
      const ref = nodes[0]!.getGrain(IOrdered, "m1");
      expect(await ref.read()).toBe("initial");

      const moved = await nodes[0]!.migrateGrain(
        new GrainId(getGrainMetadata(OrderedGrain)!.grainType, "m1"),
        addresses[1]!,
      );
      expect(moved).toBe(true);

      expect(events.order).toEqual(["onDeactivate", "dehydrate"]);
      expect(await ref.read()).toBe("set-in-onDeactivate");
      expect(events.ticksAfterShipped).toBe(0);
      expect(unbound.filter((u) => u.silo === 0)).toHaveLength(1);
    } finally {
      await Promise.all(nodes.map((n) => n.stop()));
    }
  });
});
