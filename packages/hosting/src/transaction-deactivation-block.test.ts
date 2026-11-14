import { describe, expect, it } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { IManagementGrain } from "@thresh/core/management-grain";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  deposit(cents: number): Promise<void>;
  balance(): Promise<number>;
  ping(): Promise<string>;
}

interface Teller extends GrainKey<string> {
  /** Credit `to`, then wait on the test's gate before the transaction resolves. */
  depositThenWait(to: string, cents: number): Promise<void>;
}

const Account = defineGrainInterface<Account>("BlockTxAccount", {
  options: { deposit: { transaction: "join" }, balance: { transaction: "createOrJoin" } },
});
const Teller = defineGrainInterface<Teller>("BlockTxTeller", {
  options: { depositThenWait: { transaction: "create" } },
});

let gate: Promise<void> = Promise.resolve();
/** Opens `gate`; called again on teardown so a failed test never leaves a turn parked. */
let openGate: () => void = () => undefined;

const AccountGrain = defineGrain<Account>(
  "BlockTxAccount",
  () => {
    const bal = useTransactionalState<Balance>("balance", { initial: () => ({ cents: 0 }) });
    return {
      deposit: (cents) =>
        bal.performUpdate((s) => {
          s.cents += cents;
        }),
      balance: () => bal.performRead((s) => s.cents),
      ping: async () => "pong",
    };
  },
  { placement: "preferLocal" },
);

const TellerGrain = defineGrain<Teller>(
  "BlockTxTeller",
  (ctx) => ({
    depositThenWait: async (to, cents) => {
      await ctx.getGrain(Account, to).deposit(cents);
      await gate;
    },
  }),
  { placement: "preferLocal" },
);

const accountId = (key: string) => new GrainId("BlockTxAccount" as GrainType, key);

function buildCluster(initialSilos: number, time?: FakeTimeProvider) {
  return TestCluster.start({
    clusterId: "tx-block",
    initialSilos,
    ...(time !== undefined ? { time } : {}),
    grains: [
      { ctor: AccountGrain.grain, interfaces: [Account] },
      { ctor: TellerGrain.grain, interfaces: [Teller] },
    ],
  });
}

function collectIdle(silo: TestSiloHandle): Promise<void> {
  return silo.host.getGrain(IManagementGrain, 0n).forceActivationCollection({ ms: 0 });
}

/** Start a transaction that holds `account`'s write lock until the returned release is called. */
async function holdLock(silo: TestSiloHandle, account: string) {
  gate = new Promise<void>((r) => (openGate = r));
  const done = silo.host.getGrain(Teller, `holder-${account}`).depositThenWait(account, 5);
  // Let the credit land: the account now holds the lock, idle between turns.
  for (let i = 0; i < 20 && !silo.host.isActive(accountId(account)); i++) {
    await new Promise((r) => setTimeout(r, 5));
  }
  await new Promise((r) => setTimeout(r, 20));
  return {
    release: async () => {
      openGate();
      await done;
    },
  };
}

// Orleans `ActivationLifetime.OnStopAsync`: a transaction lock or an undecided
// prepared record DELAYS a deactivation by at most 5 seconds — it does not
// exempt the activation from collection or migration.
describe("a transaction lock or an unresolved prepared record delays deactivation (T6)", () => {
  it("a forced idle collection waits for the transaction holding the lock, which commits", async () => {
    const cluster = await buildCluster(1);
    try {
      const silo = cluster.primary;
      const held = await holdLock(silo, "X");

      let collected = false;
      const collecting = collectIdle(silo).then(() => (collected = true));
      await new Promise((r) => setTimeout(r, 50));
      expect(collected).toBe(false); // waiting on the lock, inside the bound

      // The transaction resolves on the deactivating activation, then it goes.
      await held.release();
      await collecting;
      expect(silo.host.isActive(accountId("X"))).toBe(false);
      // Its write committed: the next activation reads it back.
      expect(await silo.host.getGrain(Account, "X").balance()).toBe(5);
    } finally {
      openGate();
      await cluster.dispose();
    }
  });

  it("a migration waits for the transaction holding the lock, then moves", async () => {
    const cluster = await buildCluster(2);
    try {
      const [source, target] = [cluster.silos[0]!, cluster.silos[1]!];
      const held = await holdLock(source, "Y");

      const migrating = cluster.migrate(accountId("Y"), target.address);
      await new Promise((r) => setTimeout(r, 50));
      await held.release();

      expect(await migrating).toBe(true);
      expect(source.host.isActive(accountId("Y"))).toBe(false);
      expect(target.host.isActive(accountId("Y"))).toBe(true);
    } finally {
      openGate();
      await cluster.dispose();
    }
  });

  it("a forced idle collection proceeds after the 5-second bound when a prepared record stays in doubt", async () => {
    const time = new FakeTimeProvider();
    const cluster = await buildCluster(1, time);
    try {
      // A prepared record whose TM is a grain type nobody hosts: every status
      // query fails, so the record stays in doubt.
      await cluster.transactionalStorage.store(
        "balance",
        accountId("Z"),
        undefined,
        { timeStamp: 1, commitRecords: {} },
        [
          {
            sequenceId: 1,
            transactionId: "T-in-doubt",
            timeStamp: 1,
            transactionManager: {
              grainId: new GrainId("GhostLedger" as GrainType, "tm"),
              stateName: "ledger",
            },
            state: { cents: 250 },
          },
        ],
        undefined,
        undefined,
      );
      const silo = cluster.primary;
      await silo.host.getGrain(Account, "Z").ping();

      let collected = false;
      const collecting = collectIdle(silo).then(() => (collected = true));
      for (let waited = 0; waited < 4_900; waited += 100) {
        time.advance(100);
        await new Promise((r) => setTimeout(r, 0));
      }
      expect(collected).toBe(false);

      for (let waited = 0; waited < 200 && !collected; waited += 10) {
        time.advance(10);
        await new Promise((r) => setTimeout(r, 0));
      }
      await collecting;
      expect(silo.host.isActive(accountId("Z"))).toBe(false);
    } finally {
      await cluster.dispose();
    }
  });
});
