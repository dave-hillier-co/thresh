import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { buildMigrationCluster, siloAddress } from "@thresh/example-migration/cluster";
import { runMigrationDemo } from "@thresh/example-migration/demo";
import { CartGrain } from "@thresh/example-migration/cart-grain";

const flush = () => new Promise((r) => setTimeout(r, 0));
async function settleUntil(pred: () => boolean, max = 200): Promise<void> {
  for (let i = 0; i < max && !pred(); i++) await flush();
  await flush();
}

const cartId = new GrainId("Cart", "cart-1");

describe("grain migration (live activation move acceptance)", () => {
  it("moves a live activation to another silo, carrying even unflushed state", async () => {
    const cluster = buildMigrationCluster();
    await cluster.start();
    const [node0, node1] = cluster.silos;
    try {
      // random->0 places the cart on silo-0; persist "apple", add "pear" in memory only.
      await node0!.getGrain(CartGrain, "cart-1").add("apple");
      await node0!.getGrain(CartGrain, "cart-1").checkout();
      await node0!.getGrain(CartGrain, "cart-1").add("pear");
      expect(node0!.isActive(cartId)).toBe(true);

      await node0!.getGrain(CartGrain, "cart-1").moveTo(siloAddress(1));
      cluster.time.advance(31_000);
      await settleUntil(() => node1!.isActive(cartId));

      // The activation moved silos, and the unpersisted "pear" came with it.
      expect(node0!.isActive(cartId)).toBe(false);
      expect(node1!.isActive(cartId)).toBe(true);
      expect(await node1!.getGrain(CartGrain, "cart-1").items()).toEqual(["apple", "pear"]);
    } finally {
      await cluster.stop();
    }
  });

  it("runs the runnable demo end-to-end", async () => {
    const result = await runMigrationDemo();
    expect(result.hostBefore).toBe(0);
    expect(result.hostAfter).toBe(1);
    expect(result.itemsAfterMove).toEqual(["apple", "pear"]);
  });
});
