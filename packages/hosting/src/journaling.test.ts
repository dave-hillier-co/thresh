import { describe, expect, it } from "vitest";
import { durableList, grain } from "@tsva/core/decorators";
import { defineGrain, useDurableDictionary } from "@tsva/core/define-grain";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { DurableList } from "@tsva/core/durable-state";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { MemoryJournalStorage } from "@tsva/journaling/memory-journal-storage";
import { createSilo, type SiloConfig } from "@tsva/hosting/silo-builder";

// A class grain journalling an ordered list.
interface ICart extends GrainWithStringKey {
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
interface IInventory extends GrainWithStringKey {
  stock(sku: string, qty: number): Promise<void>;
  qty(sku: string): Promise<number | undefined>;
  skus(): Promise<number>;
}
const IInventory = defineGrainInterface<IInventory>("IInventory", {
  options: { qty: { readOnly: true }, skus: { readOnly: true } },
});

const InventoryGrain = defineGrain<IInventory>("Inventory", (ctx) => {
  const stock = useDurableDictionary<string, number>(ctx, "stock");
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
    .registerGrain(InventoryGrain, { interfaces: [IInventory] })
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
