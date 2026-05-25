import { defineGrain, useReducerState } from "@tsva/core/define-grain";
import {
  IAccount,
  initialAccount,
  reduceAccount,
  type AccountEvent,
  type AccountState,
} from "@tsva/example-bank/interfaces";

/**
 * The bank account of `account-grain.ts`, written in the functional style: a
 * factory closure instead of a class, `useReducerState` instead of the
 * `@reducerState` field decorator, and `ctx` threaded explicitly instead of
 * `this`. The reducer authoring (`initialAccount` / `reduceAccount`) is shared
 * verbatim — only the grain shell changes. It registers exactly as a class grain
 * does, so the two styles coexist behind one runtime.
 */
export const AccountGrain = defineGrain<IAccount>("Account", (ctx) => {
  const state = useReducerState<AccountState, AccountEvent>(ctx, "account", {
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
