import { call, defineReducerGrain, type ReducerResult } from "@tsva/core/define-reducer-grain";
import { initialAccount, type AccountState } from "@tsva/example-bank/interfaces";

/**
 * The bank account as a single-dispatch reducer grain: its entire
 * public surface is `dispatch(action)` + a read-only `query()`, so there is no
 * per-grain interface method table to hand-write or generate — the `Action`
 * union below is the protocol. Cross-grain work (the transfer's credit leg) is
 * an Elm-style *effect* the pure reducer returns as data; the runtime runs it
 * after folding and persisting the snapshot.
 */
export type AccountAction =
  | { type: "deposit"; cents: number }
  | { type: "withdraw"; cents: number }
  | { type: "transferTo"; to: string; cents: number };

function reduce(state: AccountState, action: AccountAction): ReducerResult<AccountState> {
  switch (action.type) {
    case "deposit": {
      if (action.cents <= 0) throw new Error("deposit must be positive");
      return {
        state: {
          ...state,
          balanceCents: state.balanceCents + action.cents,
          deposits: state.deposits + 1,
        },
      };
    }
    case "withdraw": {
      if (action.cents <= 0) throw new Error("withdrawal must be positive");
      if (action.cents > state.balanceCents) throw new Error("insufficient funds");
      return {
        state: {
          ...state,
          balanceCents: state.balanceCents - action.cents,
          withdrawals: state.withdrawals + 1,
        },
      };
    }
    case "transferTo": {
      if (action.cents <= 0) throw new Error("transfer must be positive");
      if (action.cents > state.balanceCents) throw new Error("insufficient funds");
      // Pure: fold the local debit, emit the credit as an effect to run after.
      const next = {
        ...state,
        balanceCents: state.balanceCents - action.cents,
        withdrawals: state.withdrawals + 1,
      };
      return {
        state: next,
        effects: [call(Account, action.to, { type: "deposit", cents: action.cents })],
      };
    }
  }
}

export const Account = defineReducerGrain<AccountState, AccountAction>("AccountReducer", {
  initial: initialAccount,
  reduce,
});
