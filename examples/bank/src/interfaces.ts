import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/** Past-tense facts a reducer folds into account state. Never persisted in snapshot mode. */
export type AccountEvent =
  | { kind: "deposited"; cents: number }
  | { kind: "withdrawn"; cents: number };

/** The folded, immutable account state. */
export interface AccountState {
  balanceCents: number;
  deposits: number;
  withdrawals: number;
}

export const initialAccount = (): AccountState => ({
  balanceCents: 0,
  deposits: 0,
  withdrawals: 0,
});

/** Pure fold — no I/O, no grain calls, returns new immutable state. */
export const reduceAccount = (state: AccountState, event: AccountEvent): AccountState =>
  event.kind === "deposited"
    ? { ...state, balanceCents: state.balanceCents + event.cents, deposits: state.deposits + 1 }
    : {
        ...state,
        balanceCents: state.balanceCents - event.cents,
        withdrawals: state.withdrawals + 1,
      };

/** An account aggregate keyed by account number. */
export type Account = GrainKey<string> & {
  deposit(cents: number): Promise<number>;
  withdraw(cents: number): Promise<number>;
  /** Move money to another account (two-step, not an atomic distributed transaction). */
  transferTo(other: string, cents: number): Promise<number>;
  statement(): Promise<AccountState>;
};

export const account = defineGrainInterface<Account>("example.bank.account", {
  options: { statement: { readOnly: true } },
});
