import { afterEach, describe, expect, it } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { TransactionInDoubtError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import type {
  PendingTransactionState,
  TransactionalStateMetadata,
  TransactionalStateStorage,
  TransactionalStorageLoadResponse,
} from "@thresh/core/transactional-storage";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";

/**
 * The transaction manager drives the commit (T1) across silos that die: the
 * caller's, the TM's, a participant's. Every scenario must end with every
 * participant agreeing, nothing left in doubt, and the TM's commit record
 * collected (T4).
 */

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  deposit(cents: number): Promise<void>;
  balance(): Promise<number>;
}

interface Teller extends GrainKey<string> {
  fund(account: string, cents: number): Promise<void>;
  /** Credit `to` (so it is the elected TM), debit `from`, then run the test's hook. */
  transfer(from: string, to: string, cents: number): Promise<void>;
}

const Account = defineGrainInterface<Account>("TmClusterAccount", {
  options: {
    deposit: { transaction: "join" },
    balance: { transaction: "createOrJoin" },
  },
});
const Teller = defineGrainInterface<Teller>("TmClusterTeller", {
  options: {
    fund: { transaction: "create" },
    transfer: { transaction: "create" },
  },
});

/** Runs at the end of a transfer's body, just before the agent resolves it. */
let beforeResolve: () => Promise<void> | void = () => undefined;

const AccountGrain = defineGrain<Account>(
  "TmClusterAccount",
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
  "TmClusterTeller",
  (ctx) => ({
    fund: async (account, cents) => {
      await ctx.getGrain(Account, account).deposit(cents);
    },
    transfer: async (from, to, cents) => {
      await ctx.getGrain(Account, to).deposit(cents);
      await ctx.getGrain(Account, from).deposit(-cents);
      await beforeResolve();
    },
  }),
  { placement: "preferLocal" },
);

type StoreHook = (
  grainKey: string,
  metadata: TransactionalStateMetadata,
  prepares: ReadonlyArray<PendingTransactionState<unknown>>,
) => Promise<void> | void;

/** The cluster's shared transactional storage, with hooks around each store. */
class HookedStorage implements TransactionalStateStorage {
  before: StoreHook = () => undefined;
  after: StoreHook = () => undefined;

  constructor(private readonly inner: TransactionalStateStorage) {}

  load(stateName: string, grainId: GrainId): Promise<TransactionalStorageLoadResponse<unknown>> {
    return this.inner.load(stateName, grainId);
  }

  async store(
    stateName: string,
    grainId: GrainId,
    expectedETag: string | undefined,
    metadata: TransactionalStateMetadata,
    statesToPrepare: ReadonlyArray<PendingTransactionState<unknown>>,
    commitUpTo: number | undefined,
    abortAfter: number | undefined,
  ): Promise<string> {
    const key = String(grainId.key);
    await this.before(key, metadata, statesToPrepare);
    const etag = await this.inner.store(
      stateName,
      grainId,
      expectedETag,
      metadata,
      statesToPrepare,
      commitUpTo,
      abortAfter,
    );
    await this.after(key, metadata, statesToPrepare);
    return etag;
  }
}

const accountId = (key: string) => new GrainId("TmClusterAccount" as GrainType, key);

const clusters: TestCluster[] = [];
afterEach(async () => {
  beforeResolve = () => undefined;
  for (const c of clusters.splice(0)) await c.dispose();
});

async function start(): Promise<{ cluster: TestCluster; storage: HookedStorage }> {
  let storage: HookedStorage | undefined;
  const cluster = await TestCluster.start({
    clusterId: "tx-tm-cluster",
    initialSilos: 3,
    grains: [
      { ctor: AccountGrain.grain, interfaces: [Account] },
      { ctor: TellerGrain.grain, interfaces: [Teller] },
    ],
    callTimeout: { ms: 1_000 },
    // The caller's own (same-silo) call must outlive the protocol call it makes, so that the
    // outcome the agent reports — in doubt, when the TM's answer is lost — reaches the caller
    // rather than the caller's deadline firing at the same instant as the inner one.
    defaultResponseTimeout: { seconds: 10 },
    transactionsOptions: {
      remoteTransactionPingFrequencyMs: 200,
      lockTimeoutMs: 1_000,
      prepareTimeoutMs: 3_000,
      confirmationRetryDelayMs: 200,
    },
    configureSilo: (builder, _silo, c) => {
      storage ??= new HookedStorage(c.transactionalStorage);
      builder.useMemoryTransactionalStorage(storage);
    },
  });
  clusters.push(cluster);
  return { cluster, storage: storage! };
}

/**
 * The caller (a teller) on silo 0, the TM `A` on silo 1, the participant `B`
 * on silo 2, each funded with 100.
 */
async function placed(cluster: TestCluster) {
  const [caller, tmSilo, participantSilo] = cluster.silos as [
    TestSiloHandle,
    TestSiloHandle,
    TestSiloHandle,
  ];
  await tmSilo.host.getGrain(Teller, "t1").fund("A", 100);
  await participantSilo.host.getGrain(Teller, "t2").fund("B", 100);
  expect(tmSilo.host.isActive(accountId("A"))).toBe(true);
  expect(participantSilo.host.isActive(accountId("B"))).toBe(true);
  return { caller, tmSilo, participantSilo };
}

/** Wait until A and B hold `a` and `b` committed, with nothing in doubt or uncollected. */
async function converged(cluster: TestCluster, a: number, b: number): Promise<void> {
  const storage = cluster.transactionalStorage;
  await waitFor(
    async () => {
      const [ra, rb] = await Promise.all([
        storage.load("balance", accountId("A")),
        storage.load("balance", accountId("B")),
      ]);
      return (
        (ra.committedState as Balance | undefined)?.cents === a &&
        (rb.committedState as Balance | undefined)?.cents === b &&
        ra.pendingStates.length === 0 &&
        rb.pendingStates.length === 0 &&
        Object.keys(ra.metadata.commitRecords).length === 0 &&
        Object.keys(rb.metadata.commitRecords).length === 0
      );
    },
    { timeoutMs: 8_000 },
  );
}

const hasCommitRecord = (metadata: TransactionalStateMetadata) =>
  Object.keys(metadata.commitRecords).length > 0;

describe("the transaction manager drives the commit across silo failures (T1, T4)", () => {
  it("collects every commit record once the participants have confirmed", async () => {
    const { cluster } = await start();
    const { caller } = await placed(cluster);
    for (let i = 0; i < 20; i++) {
      await caller.host.getGrain(Teller, "t0").transfer("B", "A", 1);
    }
    await converged(cluster, 120, 80);
  }, 20_000);

  it("completes the transaction when the caller's silo dies after the TM accepted it", async () => {
    const { cluster, storage } = await start();
    const { caller } = await placed(cluster);
    let killed = false;
    storage.before = async (key, metadata) => {
      if (key === "A" && hasCommitRecord(metadata) && !killed) {
        killed = true;
        await cluster.killSilo(caller);
      }
    };

    void caller.host
      .getGrain(Teller, "t0")
      .transfer("B", "A", 30)
      .catch(() => undefined);

    await waitFor(() => killed);
    await converged(cluster, 130, 70);
  }, 20_000);

  it(
    "rolls the participant back when the caller's silo dies before prepareAndCommit " +
      "reached the TM, and leaves no lock held",
    async () => {
      const { cluster, storage } = await start();
      const { caller, tmSilo, participantSilo } = await placed(cluster);
      // The request to the TM is lost; the one-way prepare to B is not.
      beforeResolve = () => {
        beforeResolve = () => undefined;
        cluster.faults.drop({ from: caller.address, to: tmSilo.address });
      };
      let killed = false;
      storage.after = async (key, _metadata, prepares) => {
        if (key === "B" && prepares.length > 0 && !killed) {
          killed = true;
          await cluster.killSilo(caller);
        }
      };

      void caller.host
        .getGrain(Teller, "t0")
        .transfer("B", "A", 30)
        .catch(() => undefined);

      await waitFor(() => killed);
      await converged(cluster, 100, 100);
      // Neither account is still locked.
      await participantSilo.host.getGrain(Teller, "t3").transfer("B", "A", 5);
      await converged(cluster, 105, 95);
    },
    20_000,
  );

  it(
    "converges when the TM's silo dies after the commit is durable and before it confirmed: " +
      "the re-activated TM resends the confirmation and collects the record",
    async () => {
      const { cluster, storage } = await start();
      const { caller, tmSilo } = await placed(cluster);
      let killed = false;
      storage.after = async (key, metadata) => {
        if (key === "A" && hasCommitRecord(metadata) && !killed) {
          killed = true;
          await cluster.killSilo(tmSilo);
        }
      };

      await expect(
        caller.host.getGrain(Teller, "t0").transfer("B", "A", 30),
      ).rejects.toBeInstanceOf(TransactionInDoubtError);
      await converged(cluster, 130, 70);
    },
    20_000,
  );

  it("converges when the participant's silo dies after it reported prepared", async () => {
    const { cluster, storage } = await start();
    const { caller, participantSilo } = await placed(cluster);
    let killed = false;
    // The TM writes its commit only once B's vote has arrived.
    storage.before = async (key, metadata) => {
      if (key === "A" && hasCommitRecord(metadata) && !killed) {
        killed = true;
        await cluster.killSilo(participantSilo);
      }
    };

    await caller.host.getGrain(Teller, "t0").transfer("B", "A", 30);
    expect(killed).toBe(true);
    await converged(cluster, 130, 70);
  }, 20_000);
});
