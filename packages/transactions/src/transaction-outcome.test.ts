import { describe, expect, it } from "vitest";
import { TransactionAbortedError } from "@thresh/core/errors";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import type { TransactionsOptions } from "@thresh/transactions/transactions-options";
import {
  account,
  accountId,
  asRemote,
  dispatcherOver,
  inTransaction,
  openAccount,
  settle,
  TransactionNetwork,
  type Balance,
} from "@thresh/transactions/test-support/transaction-network";

const options: Partial<TransactionsOptions> = {
  prepareTimeoutMs: 20_000,
  remoteTransactionPingFrequencyMs: 60_000,
  confirmationRetryDelayMs: 30_000,
};

/** A transfer `a` (the TM) → `b`, with a third writer `c` when asked; writes done. */
async function transfer(withC = false) {
  const clock = new FakeTimeProvider();
  const network = new TransactionNetwork();
  const storage = new MemoryTransactionalStorage();
  const a = await openAccount(network, storage, "a", clock, options);
  const b = await openAccount(network, storage, "b", clock, options);
  const agent = new TransactionAgent(clock);
  const tx = agent.startTransaction();
  await inTransaction(tx, () => a.performUpdate((s) => (s.cents -= 30)));
  await inTransaction(tx, () => b.performUpdate((s) => (s.cents += 30)));
  if (withC) {
    const c = await openAccount(network, storage, "c", clock, options);
    await inTransaction(tx, () => c.performUpdate((s) => (s.cents += 1)));
  }
  const writers = withC ? [account("a"), account("b"), account("c")] : [account("a"), account("b")];
  const one = { reads: 0, writes: 1 };
  return {
    clock,
    network,
    storage,
    a,
    b,
    agent,
    tx,
    prepareB: () => b.prepare(tx.id, one, tx.timeStamp, account("a")),
    prepareAndCommit: () => a.prepareAndCommit(tx.id, one, tx.timeStamp, writers, writers.length),
    reopen: (key: string) => openAccount(network, storage, key, clock, options),
  };
}

async function committed(storage: MemoryTransactionalStorage, key: string) {
  const loaded = await storage.load("balance", accountId(key));
  return {
    cents: loaded.committedSequenceId === 0 ? 100 : (loaded.committedState as Balance).cents,
    pending: loaded.pendingStates.length,
    commitRecords: Object.keys(loaded.metadata.commitRecords).length,
  };
}

describe("a replaced activation converges on the TM's outcome (T1, T4)", () => {
  it(
    "a participant replaced after it voted pings on re-activation, and the TM's resent " +
      "confirmation commits it",
    async () => {
      const { clock, network, storage, b, prepareB, prepareAndCommit, reopen } = await transfer();
      await prepareB();
      await settle();
      expect(network.messages("prepared", account("a")).map((m) => m.args[3])).toEqual(["ok"]);

      // B's activation goes away after its vote.
      b.unload();
      network.takeDown(account("b"));
      await expect(prepareAndCommit()).resolves.toBe("ok");
      await settle();
      // The TM's confirm could not reach it; the commit record stays.
      expect(await committed(storage, "a")).toEqual({ cents: 70, pending: 0, commitRecords: 1 });

      // Re-activated, B queues its prepared record again and pings at once.
      const replacement = await reopen("b");
      await settle();
      expect(network.messages("ping", account("a")).length).toBe(1);
      expect(replacement.blocksDeactivation).toBe(true);

      // The TM is still confirming, so the ping needs no answer: the resent
      // confirmation commits B, and the TM then collects its record.
      clock.advance(30_000);
      await settle();
      expect(await committed(storage, "b")).toEqual({ cents: 130, pending: 0, commitRecords: 0 });
      expect(await committed(storage, "a")).toEqual({ cents: 70, pending: 0, commitRecords: 0 });
      expect(replacement.blocksDeactivation).toBe(false);
    },
  );

  it("a participant replaced after it voted learns by pinging that the TM aborted", async () => {
    const { clock, network, storage, b, prepareB, prepareAndCommit, reopen } = await transfer(true);
    void prepareB();
    // `c` never votes: the TM will time out.
    const answer = prepareAndCommit();
    await settle();
    b.unload();
    network.takeDown(account("b"));
    clock.advance(20_000);
    await settle();
    await expect(answer).resolves.toBe("prepareTimeout");
    // The TM's cancel could not reach B: its prepared record is still in storage.
    expect(await committed(storage, "b")).toMatchObject({ cents: 100, pending: 1 });

    await reopen("b");
    await settle();
    expect(network.messages("ping", account("a")).length).toBe(1);
    expect(network.messages("cancel", account("b")).map((m) => m.args[2])).toContain(
      "presumedAbort",
    );
    expect(await committed(storage, "b")).toEqual({ cents: 100, pending: 0, commitRecords: 0 });
  });

  it(
    "the TM replaced after its commit was durable, before it confirmed, resends the " +
      "confirmation from its commit record and then collects it",
    async () => {
      const { network, storage, a, prepareB, prepareAndCommit, reopen } = await transfer();
      // Nothing the TM sends reaches anyone from here on: its activation is going.
      network.dropWhen = (m) => m.method === "confirm";
      void prepareB();
      await expect(prepareAndCommit()).resolves.toBe("ok");
      await settle();
      a.unload();
      network.takeDown(account("a"));
      expect(await committed(storage, "a")).toEqual({ cents: 70, pending: 0, commitRecords: 1 });
      expect(await committed(storage, "b")).toMatchObject({ cents: 100, pending: 1 });

      network.dropWhen = () => false;
      await reopen("a");
      await settle();

      expect(await committed(storage, "b")).toEqual({ cents: 130, pending: 0, commitRecords: 0 });
      expect(await committed(storage, "a")).toEqual({ cents: 70, pending: 0, commitRecords: 0 });
    },
  );

  it("aborts when the TM's activation was replaced before the commit request reached it", async () => {
    const { network, storage, a, agent, tx, reopen } = await transfer();
    agent.setDispatcher(dispatcherOver(network));
    asRemote(tx, account("a"));
    a.unload();
    await reopen("a");

    // The fresh TM finds no lock for the transaction: brokenLock, a definite
    // abort, so the agent cancels B.
    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionAbortedError);
    await settle();
    expect(await committed(storage, "a")).toEqual({ cents: 100, pending: 0, commitRecords: 0 });
    expect(await committed(storage, "b")).toEqual({ cents: 100, pending: 0, commitRecords: 0 });
  });
});
