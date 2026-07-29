import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";

/**
 * A bank account whose balance is **transactional** state. Unlike the
 * reducer account's `transferTo` — a two-step move that can half-apply — a
 * transfer here runs as one cross-grain ACID transaction: debit and credit
 * commit together or not at all, and a failed debit rolls the credit back.
 */

interface Balance {
  cents: number;
}

export interface ITxAccount extends GrainWithStringKey {
  deposit(cents: number): Promise<void>;
  withdraw(cents: number): Promise<void>;
  balance(): Promise<number>;
}

export const ITxAccount = defineGrainInterface<ITxAccount>("example.bank.ITxAccount", {
  options: {
    deposit: { transaction: "join" },
    withdraw: { transaction: "join" },
    balance: { transaction: "createOrJoin", readOnly: true },
  },
});

export interface ITeller extends GrainWithStringKey {
  open(account: string, cents: number): Promise<void>;
  transfer(from: string, to: string, cents: number): Promise<void>;
}

export const ITeller = defineGrainInterface<ITeller>("example.bank.ITeller", {
  options: {
    open: { transaction: "create" },
    transfer: { transaction: "create" },
  },
});

export const TxAccountGrain = defineGrain<ITxAccount>("TxAccount", () => {
  const balance = useTransactionalState<Balance>("balance", {
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

export const TellerGrain = defineGrain<ITeller>("Teller", (ctx) => ({
  open: async (account, cents) => {
    await ctx.getGrain(ITxAccount, account).deposit(cents);
  },
  // One transaction spanning both accounts: credit then debit. If the debit
  // overdraws and throws, the whole transaction aborts and the credit is undone.
  transfer: async (from, to, cents) => {
    await ctx.getGrain(ITxAccount, to).deposit(cents);
    await ctx.getGrain(ITxAccount, from).withdraw(cents);
  },
}));
