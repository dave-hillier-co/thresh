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
