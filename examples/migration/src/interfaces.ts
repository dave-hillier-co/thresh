import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import type { SiloAddress } from "@tsva/core/silo-address";

/**
 * A shopping cart whose live activation can be moved to another silo without
 * losing state — even items added since the last persist (carried in the
 * migration bag, not re-read from storage).
 */
export interface ICart extends GrainWithStringKey {
  /** Add an item to the in-memory cart WITHOUT persisting it yet. */
  add(sku: string): Promise<number>;
  /** Persist the cart to storage. */
  checkout(): Promise<string[]>;
  /** The current cart contents. */
  items(): Promise<string[]>;
  /** Ask the runtime to migrate this activation to `target` next time it goes idle. */
  moveTo(target: SiloAddress): Promise<void>;
}
export const ICart = defineGrainInterface<ICart>("ICart.migration", {
  options: { items: { readOnly: true } },
});
