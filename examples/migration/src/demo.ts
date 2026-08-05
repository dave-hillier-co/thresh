import { GrainId } from "@thresh/core/grain-id";
import { buildMigrationCluster, siloAddress } from "@thresh/example-migration/cluster";
import { cart } from "@thresh/example-migration/interfaces";

const flush = () => new Promise((r) => setTimeout(r, 0));
async function settleUntil(pred: () => boolean, max = 200): Promise<void> {
  for (let i = 0; i < max && !pred(); i++) await flush();
  await flush();
}

export interface MigrationDemoResult {
  /** Which silo (0 or 1) hosted the cart before the move. */
  hostBefore: number;
  /** Which silo hosted it after the move. */
  hostAfter: number;
  /** Cart contents read from the new host — proves state survived the move. */
  itemsAfterMove: string[];
}

/**
 * Place a cart on silo-0, add items (one persisted, one not), live-migrate the
 * activation to silo-1, and read the cart back from its new host — showing the
 * unflushed item survived the move (carried in the migration bag).
 */
export async function runMigrationDemo(): Promise<MigrationDemoResult> {
  const cluster = buildMigrationCluster();
  const cartId = new GrainId("Cart", "cart-1");
  await cluster.start();
  try {
    const [node0, node1] = cluster.silos;
    // random->0 places the cart on silo-0.
    await node0!.getGrain(cart, "cart-1").checkout(); // activate + persist empty
    await node0!.getGrain(cart, "cart-1").add("apple");
    await node0!.getGrain(cart, "cart-1").checkout(); // persist ["apple"]
    await node0!.getGrain(cart, "cart-1").add("pear"); // in memory only — NOT persisted
    const hostBefore = node0!.isActive(cartId) ? 0 : 1;

    // Schedule the move and trigger idle collection by advancing the fake clock.
    await node0!.getGrain(cart, "cart-1").moveTo(siloAddress(1));
    cluster.time.advance(31_000);
    await settleUntil(() => node1!.isActive(cartId));

    const hostAfter = node1!.isActive(cartId) ? 1 : 0;
    const itemsAfterMove = await node1!.getGrain(cart, "cart-1").items();
    return { hostBefore, hostAfter, itemsAfterMove };
  } finally {
    await cluster.stop();
  }
}
