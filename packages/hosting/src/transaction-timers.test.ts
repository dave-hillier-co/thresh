import { describe, expect, it } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { TransactionAbortedError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { IManagementGrain } from "@thresh/core/management-grain";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TransactionTimerTracker } from "@thresh/transactions/test-support/transaction-timer-tracker";
import type { TransactionsOptions } from "@thresh/transactions/transactions-options";
import { TestCluster } from "@thresh/testing/test-cluster";

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  deposit(cents: number): Promise<void>;
  balance(): Promise<number>;
}

interface Teller extends GrainKey<string> {
  /** Credit `to`, then wait on the test's gate before the transaction resolves. */
  depositThenWait(to: string, cents: number): Promise<void>;
  deposit(to: string, cents: number): Promise<void>;
}

const Account = defineGrainInterface<Account>("TimerTxAccount", {
  options: {
    deposit: { transaction: "join" },
    balance: { transaction: "createOrJoin" },
  },
});
const Teller = defineGrainInterface<Teller>("TimerTxTeller", {
  options: {
    depositThenWait: { transaction: "create" },
    deposit: { transaction: "create" },
  },
});

let gate: Promise<void> = Promise.resolve();
let openGate: () => void = () => undefined;

const AccountGrain = defineGrain<Account>(
  "TimerTxAccount",
  () => {
    const bal = useTransactionalState<Balance>("balance", { initial: () => ({ cents: 0 }) });
    return {
      deposit: (cents) =>
        bal.performUpdate((s) => {
          s.cents += cents;
        }),
      balance: () => bal.performRead((s) => s.cents),
    };
  },
  { placement: "preferLocal" },
);

const TellerGrain = defineGrain<Teller>(
  "TimerTxTeller",
  (ctx) => ({
    depositThenWait: async (to, cents) => {
      await ctx.getGrain(Account, to).deposit(cents);
      await gate;
    },
    deposit: async (to, cents) => {
      await ctx.getGrain(Account, to).deposit(cents);
    },
  }),
  { placement: "preferLocal" },
);

const accountId = (key: string) => new GrainId("TimerTxAccount" as GrainType, key);

function buildCluster(
  time: FakeTimeProvider | TransactionTimerTracker,
  options?: Partial<TransactionsOptions>,
  initialSilos = 1,
) {
  return TestCluster.start({
    clusterId: "tx-timers",
    initialSilos,
    time,
    ...(options !== undefined ? { transactionsOptions: options } : {}),
    grains: [
      { ctor: AccountGrain.grain, interfaces: [Account] },
      { ctor: TellerGrain.grain, interfaces: [Teller] },
    ],
  });
}

/** Start a transaction that holds `account`'s write lock until the returned release is called. */
async function holdLock(cluster: TestCluster, account: string) {
  gate = new Promise<void>((r) => (openGate = r));
  const silo = cluster.primary;
  const done = silo.host.getGrain(Teller, `holder-${account}`).depositThenWait(account, 5);
  done.catch(() => undefined);
  for (let i = 0; i < 20 && !silo.host.isActive(accountId(account)); i++) {
    await new Promise((r) => setTimeout(r, 5));
  }
  await new Promise((r) => setTimeout(r, 20));
  return {
    done,
    release: () => {
      openGate();
      return done;
    },
  };
}

describe("transaction timers and options through the silo", () => {
  it("takes a lock hold bound from the silo configuration: a holder past it is broken (T4)", async () => {
    const clock = new FakeTimeProvider();
    const cluster = await buildCluster(clock, { lockTimeoutMs: 200 });
    try {
      const held = await holdLock(cluster, "X");

      clock.advance(200);

      // The younger transaction would die under wait-die while the holder held
      // the lock; with the bound passed it is broken, so this commits.
      await cluster.primary.host.getGrain(Teller, "later").deposit("X", 7);
      await expect(cluster.primary.host.getGrain(Account, "X").balance()).resolves.toBe(7);

      // The broken holder finds out when it tries to resolve.
      openGate();
      await expect(held.done).rejects.toBeInstanceOf(TransactionAbortedError);
    } finally {
      openGate();
      await cluster.dispose();
    }
  });

  it("keeps the default hold bound when no option is given", async () => {
    const clock = new FakeTimeProvider();
    const cluster = await buildCluster(clock);
    try {
      const held = await holdLock(cluster, "X");
      clock.advance(200);
      await expect(
        cluster.primary.host.getGrain(Teller, "later").deposit("X", 7),
      ).rejects.toBeInstanceOf(TransactionAbortedError);
      await held.release();
    } finally {
      openGate();
      await cluster.dispose();
    }
  });

  it("leaves no transactions timer pending after a released-lock activation is collected (the unload-with-live-lock cases are in the transactions package, as collection refuses a held lock)", async () => {
    const clock = new TransactionTimerTracker(new FakeTimeProvider());
    const cluster = await buildCluster(clock);
    try {
      const held = await holdLock(cluster, "Y");
      await held.release();
      await cluster.primary.host
        .getGrain(IManagementGrain, 0n)
        .forceActivationCollection({ ms: 0 });
      expect(cluster.primary.host.isActive(accountId("Y"))).toBe(false);
      expect(clock.pendingTransactionTimers()).toEqual([]);
    } finally {
      openGate();
      await cluster.dispose();
    }
  });

  it("leaves no transactions timer pending after the silo stops with a lock held", async () => {
    const fake = new FakeTimeProvider();
    const clock = new TransactionTimerTracker(fake);
    const cluster = await buildCluster(clock, undefined, 2);
    try {
      const [coordinator, participant] = [cluster.silos[0]!, cluster.silos[1]!];
      // Z lives on the participant silo; its transaction is coordinated from the
      // other silo, which then dies, orphaning the lock Z holds.
      await participant.host.getGrain(Teller, "warm").deposit("Z", 0);
      expect(participant.host.isActive(accountId("Z"))).toBe(true);
      gate = new Promise<void>((r) => (openGate = r));
      const orphaned = coordinator.host.getGrain(Teller, "orphan").depositThenWait("Z", 5);
      orphaned.catch(() => undefined);
      await new Promise((r) => setTimeout(r, 50));
      expect(clock.pendingTransactionTimers()).not.toEqual([]);
      await cluster.killSilo(coordinator);

      // The participant still holds the orphaned lock, so its graceful stop waits out the
      // deactivation bound (Orleans' five seconds) on the fake clock before it lets go.
      let stopped = false;
      const stopping = cluster.stopSilo(participant).then(() => (stopped = true));
      for (let i = 0; i < 200 && !stopped; i++) {
        fake.advance(100);
        await new Promise((r) => setTimeout(r, 0));
      }
      await stopping;

      expect(clock.pendingTransactionTimers()).toEqual([]);
    } finally {
      openGate();
      await cluster.dispose();
    }
  });
});
