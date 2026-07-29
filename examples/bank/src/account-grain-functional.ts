import { defineGrain, useReducerState } from "@thresh/core/define-grain";
import {
  IAccount,
  initialAccount,
  reduceAccount,
  type AccountEvent,
  type AccountState,
} from "@thresh/example-bank/interfaces";

/**
 * A bank account as a multi-method functional grain: a factory closure with
 * `useReducerState`, and `ctx` threaded explicitly instead of `this`. Command
 * handlers validate, then `raise` past-tense events that the pure reducer
 * (`reduceAccount`) folds into immutable state; snapshot mode persists the folded
 * state. Compare with `account-reducer-grain.ts`, which reduces the same model to
 * a single `dispatch`/`query` surface with no per-method interface.
 */
export const AccountGrain = defineGrain<IAccount>("Account", (ctx) => {
  const state = useReducerState<AccountState, AccountEvent>("account", {
    initial: initialAccount,
    reduce: reduceAccount,
  });

  const deposit = async (cents: number): Promise<number> => {
    if (cents <= 0) throw new Error("deposit must be positive");
    state.raise({ kind: "deposited", cents });
    await state.write();
    return state.value.balanceCents;
  };

  const withdraw = async (cents: number): Promise<number> => {
    if (cents <= 0) throw new Error("withdrawal must be positive");
    if (cents > state.value.balanceCents) throw new Error("insufficient funds");
    state.raise({ kind: "withdrawn", cents });
    await state.write();
    return state.value.balanceCents;
  };

  const transferTo = async (other: string, cents: number): Promise<number> => {
    const remaining = await withdraw(cents);
    await ctx.getGrain(IAccount, other).deposit(cents);
    return remaining;
  };

  const statement = async (): Promise<AccountState> => state.value;

  return { deposit, withdraw, transferTo, statement };
});
