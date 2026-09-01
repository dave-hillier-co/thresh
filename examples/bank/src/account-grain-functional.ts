import { defineGrain, useReducerState } from "@thresh/core/define-grain";
import {
  initialAccount,
  reduceAccount,
  type AccountEvent,
  type AccountState,
} from "@thresh/example-bank/interfaces";

/**
 * An account aggregate keyed by account number. Pinned explicitly (rather than
 * inferred from the factory below) because `transferTo` calls back into this
 * same grain type via `ctx.getGrain` — the factory refers to `AccountGrain`
 * before its own definition is complete, so the contract can't be inferred
 * from its own return type.
 */
interface Account {
  deposit(cents: number): Promise<number>;
  withdraw(cents: number): Promise<number>;
  /** Move money to another account (two-step, not an atomic distributed transaction). */
  transferTo(other: string, cents: number): Promise<number>;
  statement(): Promise<AccountState>;
}

/**
 * A bank account as a multi-method functional grain: a factory closure with
 * `useReducerState`, and `ctx` threaded explicitly instead of `this`. Command
 * handlers validate, then `raise` past-tense events that the pure reducer
 * (`reduceAccount`) folds into immutable state; snapshot mode persists the folded
 * state. Compare with `account-reducer-grain.ts`, which reduces the same model to
 * a single `dispatch`/`query` surface with no per-method interface.
 */
export const AccountGrain = defineGrain<Account>(
  "Account",
  (ctx) => {
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
      await ctx.getGrain(AccountGrain, other).deposit(cents);
      return remaining;
    };

    const statement = async (): Promise<AccountState> => state.value;

    return { deposit, withdraw, transferTo, statement };
  },
  { interfaceOptions: { statement: { readOnly: true } } },
);
