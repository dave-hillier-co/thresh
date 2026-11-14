import { describe, expect, it } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { TransactionAbortedError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { TestCluster } from "@thresh/testing/test-cluster";

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  deposit(cents: number): Promise<void>;
  balance(): Promise<number>;
}

interface Teller extends GrainKey<string> {
  fund(account: string, cents: number): Promise<void>;
  /** Credit `to`, run the test's hook, then debit `from`. */
  transfer(from: string, to: string, cents: number): Promise<void>;
  /** Wait on the test's gate, then credit `to`. */
  depositAfterGate(to: string, cents: number): Promise<void>;
}

const Account = defineGrainInterface<Account>("ProtocolTxAccount", {
  options: {
    deposit: { transaction: "join" },
    balance: { transaction: "createOrJoin" },
  },
});
const Teller = defineGrainInterface<Teller>("ProtocolTxTeller", {
  options: {
    fund: { transaction: "create" },
    transfer: { transaction: "create" },
    depositAfterGate: { transaction: "create" },
  },
});

/** What the teller runs between its credit and its debit, set per test. */
let betweenCreditAndDebit: () => Promise<void> = async () => undefined;
/** What `depositAfterGate` waits on before it touches the account, set per test. */
let gate: Promise<void> = Promise.resolve();

const AccountGrain = defineGrain<Account>(
  "ProtocolTxAccount",
  () => {
    const bal = useTransactionalState<Balance>("balance", { initial: () => ({ cents: 0 }) });
    return {
      deposit: (cents) =>
        bal.performUpdate((s) => {
          if (s.cents + cents < 0) throw new Error("insufficient funds");
          s.cents += cents;
        }),
      balance: () => bal.performRead((s) => s.cents),
    };
  },
  { placement: "preferLocal" },
);

const TellerGrain = defineGrain<Teller>(
  "ProtocolTxTeller",
  (ctx) => ({
    fund: async (account, cents) => {
      await ctx.getGrain(Account, account).deposit(cents);
    },
    transfer: async (from, to, cents) => {
      await ctx.getGrain(Account, to).deposit(cents);
      await betweenCreditAndDebit();
      await ctx.getGrain(Account, from).deposit(-cents);
    },
    depositAfterGate: async (to, cents) => {
      await gate;
      await ctx.getGrain(Account, to).deposit(cents);
    },
  }),
  { placement: "preferLocal" },
);

const accountId = (key: string) => new GrainId("ProtocolTxAccount" as GrainType, key);

function buildCluster(initialSilos = 2) {
  return TestCluster.start({
    clusterId: "tx-protocol",
    initialSilos,
    grains: [
      { ctor: AccountGrain.grain, interfaces: [Account] },
      { ctor: TellerGrain.grain, interfaces: [Teller] },
    ],
  });
}

function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => (resolve = r));
  return { promise, resolve };
}

describe("transaction protocol across silos", () => {
  it("aborts a transfer whose remote participant's silo dies after its write returns (T2)", async () => {
    const cluster = await buildCluster(3);
    try {
      const [home, doomed] = [cluster.silos[0]!, cluster.silos[1]!];
      await home.host.getGrain(Teller, "t0").fund("A", 100);
      await doomed.host.getGrain(Teller, "t1").fund("B", 100);
      expect(doomed.host.isActive(accountId("B"))).toBe(true);

      // B's credit has returned; its silo dies before the transaction prepares.
      betweenCreditAndDebit = async () => {
        betweenCreditAndDebit = async () => undefined;
        await cluster.killSilo(doomed);
      };

      await expect(home.host.getGrain(Teller, "t0").transfer("A", "B", 30)).rejects.toBeInstanceOf(
        TransactionAbortedError,
      );
      expect(await home.host.getGrain(Account, "A").balance()).toBe(100);
      expect(await home.host.getGrain(Account, "B").balance()).toBe(100);
    } finally {
      betweenCreditAndDebit = async () => undefined;
      await cluster.dispose();
    }
  });

  it("runs an abort while the participant's own turn waits for the lock the abort releases (T4)", async () => {
    const cluster = await buildCluster();
    try {
      const [home, remote] = [cluster.silos[0]!, cluster.silos[1]!];
      // The contended account lives on the other silo, so the protocol calls
      // reach it as messages, not as direct calls on a local participant.
      await remote.host.getGrain(Teller, "warm").fund("X", 0);
      await home.host.getGrain(Teller, "fund-a").fund("A", 0);

      // The older transaction starts first and parks before touching X.
      const opened = deferred();
      gate = opened.promise;
      const older = home.host.getGrain(Teller, "older").depositAfterGate("X", 5);

      // The younger one takes X's write lock, then fails on its debit of A
      // (insufficient funds) and aborts — but only once the older one is
      // already waiting for X's lock inside X's turn.
      const olderWaiting = deferred();
      betweenCreditAndDebit = async () => {
        betweenCreditAndDebit = async () => undefined;
        opened.resolve();
        await new Promise((r) => setTimeout(r, 50));
        olderWaiting.resolve();
      };
      const younger = home.host.getGrain(Teller, "younger").transfer("A", "X", 1);
      await olderWaiting.promise;

      // The abort released X's lock while the older turn waited — well inside
      // the 8 s hold bound and the 10 s acquire bound that would otherwise
      // have freed it.
      const outcome = await Promise.race([
        older.then(() => "committed"),
        new Promise((r) => setTimeout(() => r("still blocked"), 2_000)),
      ]);
      expect(outcome).toBe("committed");
      await expect(younger).rejects.toThrow(/insufficient/);
      expect(await home.host.getGrain(Account, "X").balance()).toBe(5);
    } finally {
      gate = Promise.resolve();
      betweenCreditAndDebit = async () => undefined;
      await cluster.dispose();
    }
  }, 15_000);
});
