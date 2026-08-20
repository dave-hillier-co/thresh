import { defineGrain, usePersistentState } from "@thresh/core/define-grain";
import type { SiloAddress } from "@thresh/core/silo-address";

interface CartState {
  items: string[];
}

/**
 * A cart with a `PersistentState` facet. The facet auto-participates in live
 * migration: when `moveTo` schedules a migration, the runtime carries the cart's
 * in-memory value (including items added but not yet `checkout`-persisted) to the
 * target silo and rehydrates it there, skipping the storage read — so the move
 * never loses unflushed state. Functional-first (`defineGrain` + hooks).
 */
export const CartGrain = defineGrain(
  "Cart",
  (ctx) => {
    const cart = usePersistentState<CartState>("cart", {
      defaultValue: (): CartState => ({ items: [] }),
    });
    return {
      /** Add an item to the in-memory cart WITHOUT persisting it yet. */
      add: async (sku: string): Promise<number> => {
        cart.value.items.push(sku); // in memory only — not persisted until checkout
        return cart.value.items.length;
      },
      /** Persist the cart to storage. */
      checkout: async (): Promise<string[]> => {
        await cart.write();
        return [...cart.value.items];
      },
      /** The current cart contents. */
      items: async (): Promise<string[]> => [...cart.value.items],
      /** Ask the runtime to migrate this activation to `target` next time it goes idle. */
      moveTo: async (target: SiloAddress): Promise<void> => {
        ctx.runtime.migrateOnIdle(target);
      },
    };
  },
  { interfaceOptions: { items: { readOnly: true } } },
);
