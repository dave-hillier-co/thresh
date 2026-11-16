import { describe, expect, it } from "vitest";
import { TransactionAbortedError } from "@thresh/core/errors";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import {
  account,
  accountId,
  asRemote,
  dispatcherOver,
  inTransaction,
  openAccount,
  TransactionNetwork,
  type Balance,
} from "@thresh/transactions/test-support/transaction-network";

async function committedCents(storage: MemoryTransactionalStorage, key: string): Promise<number> {
  const loaded = await storage.load("balance", accountId(key));
  return loaded.committedSequenceId === 0 ? 100 : (loaded.committedState as Balance).cents;
}

describe("prepare validates the participant's lock (T2)", () => {
  it("aborts a transfer whose writer was re-activated between its write and its prepare", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const a = await openAccount(network, storage, "a", clock);
    const b = await openAccount(network, storage, "b", clock);
    const agent = new TransactionAgent(clock);
    agent.setDispatcher(dispatcherOver(network));

    const tx = agent.startTransaction();
    await inTransaction(tx, () => a.performUpdate((s) => (s.cents -= 30)));
    await inTransaction(tx, () => b.performUpdate((s) => (s.cents += 30)));
    asRemote(tx, account("b"));
    // B's activation is replaced before the prepare reaches it.
    b.unload();
    await openAccount(network, storage, "b", clock);

    // The replacement votes brokenLock to the TM, which aborts.
    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionAbortedError);
    expect(await committedCents(storage, "a")).toBe(100);
    expect(await committedCents(storage, "b")).toBe(100);
  });

  it("aborts when a read-only participant lost its lock before prepare", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const writer = await openAccount(network, storage, "w", clock);
    const reader = await openAccount(network, storage, "r", clock);
    const agent = new TransactionAgent(clock);
    agent.setDispatcher(dispatcherOver(network));

    const tx = agent.startTransaction();
    const seen = await inTransaction(tx, () => reader.performRead((s) => s.cents));
    await inTransaction(tx, () => writer.performUpdate((s) => (s.cents = seen + 1)));
    asRemote(tx, account("r"));
    reader.unload();
    await openAccount(network, storage, "r", clock);

    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionAbortedError);
    expect(await committedCents(storage, "w")).toBe(100);
  });

  it("refuses the commit when the access the coordinator recorded differs from what the lock saw", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const state = await openAccount(network, storage, "a", clock);
    const agent = new TransactionAgent(clock);
    const tx = agent.startTransaction();
    await inTransaction(tx, () => state.performUpdate((s) => (s.cents += 1)));

    expect(
      await state.prepareAndCommit(tx.id, { reads: 0, writes: 2 }, tx.timeStamp, [account("a")], 1),
    ).toBe("lockValidationFailed");

    // The failed validation rolled the transaction back (Orleans
    // `ValidateLock` → `Rollback`): a later writer is not blocked by it.
    const later = agent.startTransaction();
    await inTransaction(later, () => state.performUpdate((s) => (s.cents += 5)));
    await agent.resolve(later);
    expect(await committedCents(storage, "a")).toBe(105);
  });

  it("commits when the lock still holds exactly the recorded access", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const state = await openAccount(network, storage, "a", clock);
    const agent = new TransactionAgent(clock);
    const tx = agent.startTransaction();
    await inTransaction(tx, () => state.performRead((s) => s.cents));
    await inTransaction(tx, () => state.performUpdate((s) => (s.cents += 1)));

    expect(
      await state.prepareAndCommit(tx.id, { reads: 1, writes: 1 }, tx.timeStamp, [account("a")], 1),
    ).toBe("ok");
    expect(await committedCents(storage, "a")).toBe(101);
  });

  it("votes no to the TM when a participant's recorded access differs", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const tm = await openAccount(network, storage, "tm", clock);
    const b = await openAccount(network, storage, "b", clock);
    const agent = new TransactionAgent(clock);
    const tx = agent.startTransaction();
    await inTransaction(tx, () => tm.performUpdate((s) => (s.cents -= 1)));
    await inTransaction(tx, () => b.performUpdate((s) => (s.cents += 1)));

    await b.prepare(tx.id, { reads: 0, writes: 2 }, tx.timeStamp, account("tm"));
    const status = await tm.prepareAndCommit(
      tx.id,
      { reads: 0, writes: 1 },
      tx.timeStamp,
      [account("tm"), account("b")],
      2,
    );

    expect(status).toBe("lockValidationFailed");
    expect(network.messages("prepared", account("tm")).map((m) => m.args[3])).toEqual([
      "lockValidationFailed",
    ]);
    expect(await committedCents(storage, "tm")).toBe(100);
  });
});
