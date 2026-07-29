import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";

/**
 * A bank account whose balance is **transactional** state. Unlike the
 * reducer account's `transferTo` — a two-step move that can half-apply — a
 * transfer here runs as one cross-grain ACID transaction: debit and credit
 * commit together or not at all, and a failed debit rolls the credit back.
 */

interface Balance {
  cents: number;
}

export const TxAccountGrain = defineGrain(
  "TxAccount",
  () => {
    const balance = useTransactionalState<Balance>("balance", {
      initial: () => ({ cents: 0 }),
    });
    return {
      deposit: (cents: number) =>
        balance.performUpdate((s) => {
          s.cents += cents;
        }),
      withdraw: (cents: number) =>
        balance.performUpdate((s) => {
          if (s.cents < cents) throw new Error("insufficient funds");
          s.cents -= cents;
        }),
      balance: () => balance.performRead((s) => s.cents),
    };
  },
  {
    interfaceOptions: {
      deposit: { transaction: "join" },
      withdraw: { transaction: "join" },
      balance: { transaction: "createOrJoin", readOnly: true },
    },
  },
);

export const TellerGrain = defineGrain(
  "Teller",
  (ctx) => ({
    open: async (account: string, cents: number) => {
      await ctx.getGrain(TxAccountGrain, account).deposit(cents);
    },
    // One transaction spanning both accounts: credit then debit. If the debit
    // overdraws and throws, the whole transaction aborts and the credit is undone.
    transfer: async (from: string, to: string, cents: number) => {
      await ctx.getGrain(TxAccountGrain, to).deposit(cents);
      await ctx.getGrain(TxAccountGrain, from).withdraw(cents);
    },
  }),
  {
    interfaceOptions: {
      open: { transaction: "create" },
      transfer: { transaction: "create" },
    },
  },
);
