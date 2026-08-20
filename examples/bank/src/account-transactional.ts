import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

/**
 * A bank account whose balance is **transactional** state. Unlike the
 * reducer account's `transferTo` — a two-step move that can half-apply — a
 * transfer here runs as one cross-grain ACID transaction: debit and credit
 * commit together or not at all, and a failed debit rolls the credit back.
 */

interface Balance {
  cents: number;
}

export type TxAccount = GrainKey<string> & {
  deposit(cents: number): Promise<void>;
  withdraw(cents: number): Promise<void>;
  balance(): Promise<number>;
};

export const txAccount = defineGrainInterface<TxAccount>("example.bank.txAccount", {
  options: {
    deposit: { transaction: "join" },
    withdraw: { transaction: "join" },
    balance: { transaction: "createOrJoin", readOnly: true },
  },
});

export type Teller = GrainKey<string> & {
  open(account: string, cents: number): Promise<void>;
  transfer(from: string, to: string, cents: number): Promise<void>;
};

export const teller = defineGrainInterface<Teller>("example.bank.teller", {
  options: {
    open: { transaction: "create" },
    transfer: { transaction: "create" },
  },
});

export const TxAccountGrain = defineGrain<TxAccount>("TxAccount", (ctx) => {
  const balance = useTransactionalState<Balance>(ctx, "balance", {
    initial: () => ({ cents: 0 }),
  });
  return {
    deposit: (cents) =>
      balance.performUpdate((s) => {
        s.cents += cents;
      }),
    withdraw: (cents) =>
      balance.performUpdate((s) => {
        if (s.cents < cents) throw new Error("insufficient funds");
        s.cents -= cents;
      }),
    balance: () => balance.performRead((s) => s.cents),
  };
});

export const TellerGrain = defineGrain<Teller>("Teller", (ctx) => ({
  open: async (account, cents) => {
    await ctx.getGrain(txAccount, account).deposit(cents);
  },
  // One transaction spanning both accounts: credit then debit. If the debit
  // overdraws and throws, the whole transaction aborts and the credit is undone.
  transfer: async (from, to, cents) => {
    await ctx.getGrain(txAccount, to).deposit(cents);
    await ctx.getGrain(txAccount, from).withdraw(cents);
  },
}));
