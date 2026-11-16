import { describe, expect, it } from "vitest";
import { grain, transactionalState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import type { TransactionalState } from "@thresh/core/transactional-state";
import type { TransactionalStateStorage } from "@thresh/core/transactional-storage";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { createSilo } from "@thresh/hosting/silo-builder";
import { waitFor } from "@thresh/testing/wait";

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  balance(): Promise<number>;
}
interface Ledger extends GrainKey<string> {
  open(): Promise<void>;
}

const Account = defineGrainInterface<Account>("RecoveryAccount.iface", {
  options: { balance: { transaction: "createOrJoin" } },
});
const Ledger = defineGrainInterface<Ledger>("RecoveryLedger.iface", {
  options: { open: { transaction: "create" } },
});

// The participant whose state can be left in-doubt by a mid-commit crash.
@grain({ name: "RecoveryAccount" })
class AccountGrain extends Grain implements Account {
  @transactionalState("balance", { initial: (): Balance => ({ cents: 0 }) })
  private bal!: TransactionalState<Balance>;
  async balance(): Promise<number> {
    return this.bal.performRead((s) => s.cents);
  }
}

// The transaction manager: its "ledger" state holds the durable commit records.
@grain({ name: "RecoveryLedger" })
class LedgerGrain extends Grain implements Ledger {
  @transactionalState("ledger", { initial: (): Balance => ({ cents: 0 }) })
  private ledger!: TransactionalState<Balance>;
  async open(): Promise<void> {
    await this.ledger.performUpdate((s) => {
      s.cents += 1;
    });
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");
const accountId = (key: string) => new GrainId("RecoveryAccount" as GrainType, key);
const ledgerId = new GrainId("RecoveryLedger" as GrainType, "L");

function buildSilo(storage: MemoryTransactionalStorage) {
  return createSilo({ clusterId: "tx-recovery", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryTransactionalStorage(storage)
    .registerGrain(AccountGrain, { interfaces: [Account] })
    .registerGrain(LedgerGrain, { interfaces: [Ledger] })
    .build();
}

/** Seed an account with a prepared-but-uncommitted pending record (post-crash in-doubt). */
async function seedInDoubt(
  storage: TransactionalStateStorage,
  accountKey: string,
  transactionId: string,
  state: Balance,
): Promise<void> {
  await storage.store(
    "balance",
    accountId(accountKey),
    undefined,
    { timeStamp: 1, commitRecords: {} },
    [
      {
        sequenceId: 1,
        transactionId,
        timeStamp: 1,
        transactionManager: { grainId: ledgerId, stateName: "ledger" },
        state,
      },
    ],
    undefined,
    undefined,
  );
}

/**
 * Seed the TM's ledger with a durable commit record naming `participant` as a
 * write participant still to confirm (the transaction reached its commit point).
 */
async function seedCommitRecord(
  storage: TransactionalStateStorage,
  transactionId: string,
  participant: string,
): Promise<void> {
  await storage.store(
    "ledger",
    ledgerId,
    undefined,
    {
      timeStamp: 1,
      commitRecords: {
        [transactionId]: {
          timeStamp: 1,
          writeParticipants: [
            { grainId: ledgerId, stateName: "ledger" },
            { grainId: accountId(participant), stateName: "balance" },
          ],
        },
      },
    },
    [],
    undefined,
    undefined,
  );
}

/** The account's durable record, once nothing is left in doubt. */
async function settled(storage: TransactionalStateStorage, key: string) {
  await waitFor(
    async () => (await storage.load("balance", accountId(key))).pendingStates.length === 0,
  );
  return storage.load("balance", accountId(key));
}

describe("transaction in-doubt recovery (Slice 4d)", () => {
  it("commits an in-doubt record when the TM recorded the commit", async () => {
    const storage = new MemoryTransactionalStorage();
    await seedInDoubt(storage, "committed", "T1", { cents: 100 });
    await seedCommitRecord(storage, "T1", "committed");

    const silo = buildSilo(storage);
    await silo.start();
    try {
      // Activating the account queues its prepared record again and pings the
      // TM; the TM, activated, resumes confirming its commit record.
      await silo.getGrain(Account, "committed").balance();
      expect((await settled(storage, "committed")).committedState).toEqual({ cents: 100 });
      expect(await silo.getGrain(Account, "committed").balance()).toBe(100);
      await waitFor(
        async () =>
          Object.keys((await storage.load("ledger", ledgerId)).metadata.commitRecords).length === 0,
      );
    } finally {
      await silo.stop();
    }
  });

  it("aborts an in-doubt record when the TM has no commit record", async () => {
    const storage = new MemoryTransactionalStorage();
    await seedInDoubt(storage, "aborted", "T2", { cents: 100 });
    // No commit record for T2: the transaction never reached its commit point.

    const silo = buildSilo(storage);
    await silo.start();
    try {
      // The ping finds a TM that never heard of T2: presumed abort, and the
      // staged write is dropped. A read made while it was still in doubt read
      // its state and is aborted with it, so read once it has settled.
      await silo
        .getGrain(Account, "aborted")
        .balance()
        .catch(() => undefined);
      expect((await settled(storage, "aborted")).committedSequenceId).toBe(0);
      expect(await silo.getGrain(Account, "aborted").balance()).toBe(0);
    } finally {
      await silo.stop();
    }
  });
});
