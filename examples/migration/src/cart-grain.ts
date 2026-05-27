import { defineGrain, usePersistentState } from "@tsva/core/define-grain";
import { ICart } from "@tsva/example-migration/interfaces";

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
export const CartGrain = defineGrain<ICart>("Cart", (ctx) => {
  const cart = usePersistentState<CartState>(ctx, "cart", {
    defaultValue: (): CartState => ({ items: [] }),
  });
  return {
    add: async (sku) => {
      cart.value.items.push(sku); // in memory only — not persisted until checkout
      return cart.value.items.length;
    },
    checkout: async () => {
      await cart.write();
      return [...cart.value.items];
    },
    items: async () => [...cart.value.items],
    moveTo: async (target) => {
      ctx.runtime.migrateOnIdle(target);
    },
  };
});
