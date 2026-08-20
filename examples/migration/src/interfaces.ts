import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { SiloAddress } from "@thresh/core/silo-address";

/**
 * A shopping cart whose live activation can be moved to another silo without
 * losing state — even items added since the last persist (carried in the
 * migration bag, not re-read from storage).
 */
export type Cart = GrainKey<string> & {
  /** Add an item to the in-memory cart WITHOUT persisting it yet. */
  add(sku: string): Promise<number>;
  /** Persist the cart to storage. */
  checkout(): Promise<string[]>;
  /** The current cart contents. */
  items(): Promise<string[]>;
  /** Ask the runtime to migrate this activation to `target` next time it goes idle. */
  moveTo(target: SiloAddress): Promise<void>;
};
export const cart = defineGrainInterface<Cart>("cart.migration", {
  options: { items: { readOnly: true } },
});
