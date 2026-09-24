import { describe, expect, it } from "vitest";
import { durableDictionary, durableList, grain } from "@thresh/core/decorators";
import { defineGrain, useDurableDictionary } from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { DurableDictionary, DurableList } from "@thresh/core/durable-state";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryJournalStorage } from "@thresh/journaling/memory-journal-storage";
import { createSilo, type SiloConfig } from "@thresh/hosting/silo-builder";

// A class grain journalling an ordered list.
interface ICart extends GrainKey<string> {
  add(item: string): Promise<number>;
  list(): Promise<readonly string[]>;
}
const ICart = defineGrainInterface<ICart>("ICart", { options: { list: { readOnly: true } } });

@grain()
class CartGrain extends Grain implements ICart {
  @durableList("items")
  private items!: DurableList<string>;

  async add(item: string): Promise<number> {
    await this.items.add(item);
    return this.items.length;
  }

  async list(): Promise<readonly string[]> {
    return this.items.toArray();
  }
}

// A functional grain journalling a dictionary via the hook.
interface IInventory extends GrainKey<string> {
  stock(sku: string, qty: number): Promise<void>;
  qty(sku: string): Promise<number | undefined>;
  skus(): Promise<number>;
}
const IInventory = defineGrainInterface<IInventory>("IInventory", {
  options: { qty: { readOnly: true }, skus: { readOnly: true } },
});

const InventoryGrain = defineGrain<IInventory>("Inventory", () => {
  const stock = useDurableDictionary<string, number>("stock");
  return {
    stock: async (sku, qty) => {
      await stock.set(sku, qty);
    },
    qty: async (sku) => stock.get(sku),
    skus: async () => stock.size,
  };
});

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(storage: MemoryJournalStorage, config: Partial<SiloConfig> = {}) {
  return createSilo({ clusterId: "c1", local, ...config })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryJournaling(storage)
    .registerGrain(CartGrain, { interfaces: [ICart] })
    .registerGrain(InventoryGrain.grain, { interfaces: [IInventory] })
    .build();
}

describe("durable journaling end-to-end", () => {
  it("replays a journalled list across a silo restart (class grain)", async () => {
    const storage = new MemoryJournalStorage();

    const first = buildSilo(storage);
    await first.start();
    await first.getGrain(ICart, "c-1").add("apple");
    expect(await first.getGrain(ICart, "c-1").add("pear")).toBe(2);
    await first.stop(); // pod dies

    const restarted = buildSilo(storage); // new pod, same durable store
    await restarted.start();
    try {
      expect(await restarted.getGrain(ICart, "c-1").list()).toEqual(["apple", "pear"]);
    } finally {
      await restarted.stop();
    }
  });

  it("replays a journalled dictionary across a silo restart (functional grain)", async () => {
    const storage = new MemoryJournalStorage();

    const first = buildSilo(storage);
    await first.start();
    await first.getGrain(IInventory, "warehouse").stock("widget", 5);
    await first.getGrain(IInventory, "warehouse").stock("gadget", 3);
    await first.stop();

    const restarted = buildSilo(storage);
    await restarted.start();
    try {
      expect(await restarted.getGrain(IInventory, "warehouse").qty("widget")).toBe(5);
      expect(await restarted.getGrain(IInventory, "warehouse").skus()).toBe(2);
    } finally {
      await restarted.stop();
    }
  });

  it("survives a mid-life snapshot compaction and a restart", async () => {
    const storage = new MemoryJournalStorage();

    const first = buildSilo(storage, { snapshotThreshold: 3 });
    await first.start();
    const cart = first.getGrain(ICart, "big");
    for (const item of ["a", "b", "c", "d", "e"]) await cart.add(item); // crosses threshold
    await first.stop();

    const restarted = buildSilo(storage, { snapshotThreshold: 3 });
    await restarted.start();
    try {
      expect(await restarted.getGrain(ICart, "big").list()).toEqual(["a", "b", "c", "d", "e"]);
    } finally {
      await restarted.stop();
    }
  });

  it("keeps separate grain keys independent", async () => {
    const storage = new MemoryJournalStorage();
    const silo = buildSilo(storage);
    await silo.start();
    try {
      await silo.getGrain(ICart, "x").add("only-x");
      expect(await silo.getGrain(ICart, "y").list()).toEqual([]);
      expect(await silo.getGrain(ICart, "x").list()).toEqual(["only-x"]);
    } finally {
      await silo.stop();
    }
  });
});

// A durable-dictionary grain that can migrate to another silo, for the
// rehydrate-replay repro in issue #94.
interface IWarehouse extends GrainKey<string> {
  stock(sku: string, qty: number): Promise<void>;
  qty(sku: string): Promise<number | undefined>;
  scheduleMigration(target: SiloAddress): Promise<void>;
}
const IWarehouse = defineGrainInterface<IWarehouse>("IWarehouse.migration", {
  options: { qty: { readOnly: true } },
});

@grain()
class WarehouseGrain extends Grain implements IWarehouse {
  @durableDictionary("stock")
  private stockLevels!: DurableDictionary<string, number>;

  async stock(sku: string, qty: number): Promise<void> {
    await this.stockLevels.set(sku, qty);
  }

  async qty(sku: string): Promise<number | undefined> {
    return this.stockLevels.get(sku);
  }

  async scheduleMigration(target: SiloAddress): Promise<void> {
    this.runtime.migrateOnIdle(target);
  }
}

describe("durable-state migration (issue #94)", () => {
  const local0 = new SiloAddress("silo-m0", "uid-m0", "silo-m0:11111");
  const local1 = new SiloAddress("silo-m1", "uid-m1", "silo-m1:11112");
  const warehouseId = new GrainId("Warehouse", "wh-1");

  function buildMigrationSilo(
    local: SiloAddress,
    network: InProcessNetwork,
    storage: MemoryJournalStorage,
    time: FakeTimeProvider,
  ) {
    return createSilo({
      clusterId: "c-journal-migration",
      local,
      time,
      collectionAgeSeconds: 30,
      collectionIntervalSeconds: 10,
      random: () => 0,
    })
      .useStaticMembership([local0, local1])
      .useInProcessTransport(network)
      .useMemoryJournaling(storage)
      .registerGrain(WarehouseGrain, { interfaces: [IWarehouse] })
      .build();
  }

  const flush = () => new Promise((r) => setTimeout(r, 0));
  async function settleUntil(pred: () => boolean, max = 200): Promise<void> {
    for (let i = 0; i < max && !pred(); i++) await flush();
    await flush();
  }

  it("replays the journal on the target silo instead of starting empty", async () => {
    const network = new InProcessNetwork();
    const storage = new MemoryJournalStorage();
    const time = new FakeTimeProvider();
    const node0 = buildMigrationSilo(local0, network, storage, time);
    const node1 = buildMigrationSilo(local1, network, storage, time);
    await node0.start();
    await node1.start();
    try {
      await node0.getGrain(IWarehouse, "wh-1").stock("apple", 7);
      expect(node0.isActive(warehouseId)).toBe(true);

      await node0.getGrain(IWarehouse, "wh-1").scheduleMigration(local1);
      time.advance(31_000);
      await settleUntil(() => node1.isActive(warehouseId));

      expect(node0.isActive(warehouseId)).toBe(false);
      expect(node1.isActive(warehouseId)).toBe(true);

      // Rehydrated without a persistent-state migration participant for the
      // durable-dictionary facet: the target must replay the journal itself
      // rather than come up with an empty structure.
      expect(await node1.getGrain(IWarehouse, "wh-1").qty("apple")).toBe(7);

      // And further writes must succeed (no stale version/log-consistency error).
      await node1.getGrain(IWarehouse, "wh-1").stock("pear", 3);
      expect(await node1.getGrain(IWarehouse, "wh-1").qty("pear")).toBe(3);
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});
