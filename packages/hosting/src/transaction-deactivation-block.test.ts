import { describe, expect, it } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { IManagementGrain } from "@thresh/core/management-grain";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  deposit(cents: number): Promise<void>;
  ping(): Promise<string>;
}

interface Teller extends GrainKey<string> {
  /** Credit `to`, then wait on the test's gate before the transaction resolves. */
  depositThenWait(to: string, cents: number): Promise<void>;
}

const Account = defineGrainInterface<Account>("BlockTxAccount", {
  options: { deposit: { transaction: "join" } },
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

function buildCluster(initialSilos: number) {
  return TestCluster.start({
    clusterId: "tx-block",
    initialSilos,
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

describe("a transaction lock or an unresolved prepared record blocks deactivation (T6)", () => {
  it("survives a forced idle collection while it holds a transaction lock", async () => {
    const cluster = await buildCluster(1);
    try {
      const silo = cluster.primary;
      const held = await holdLock(silo, "X");

      await collectIdle(silo);
      expect(silo.host.isActive(accountId("X"))).toBe(true);

      // Once the transaction resolves nothing holds it here any more.
      await held.release();
      await collectIdle(silo);
      expect(silo.host.isActive(accountId("X"))).toBe(false);
    } finally {
      openGate();
      await cluster.dispose();
    }
  });

  it("is not migrated while it holds a transaction lock", async () => {
    const cluster = await buildCluster(2);
    try {
      const [source, target] = [cluster.silos[0]!, cluster.silos[1]!];
      const held = await holdLock(source, "Y");

      expect(await cluster.migrate(accountId("Y"), target.address)).toBe(false);
      expect(source.host.isActive(accountId("Y"))).toBe(true);
      await held.release();
    } finally {
      openGate();
      await cluster.dispose();
    }
  });

  it("survives a forced idle collection while a prepared record's outcome is unknown", async () => {
    const cluster = await buildCluster(1);
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

      await collectIdle(silo);
      expect(silo.host.isActive(accountId("Z"))).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });
});
