import { describe, expect, it } from "vitest";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import {
  accountId,
  inTransaction,
  openAccount,
  settle,
  TransactionNetwork,
} from "@thresh/transactions/test-support/transaction-network";

/**
 * The TM removes a commit record once every other write participant has
 * confirmed (Orleans `ConfirmationWorker` and `StorageBatch.Collect`); a TM
 * with no other write participant collects it in the commit write itself.
 */
describe("commit records are collected (T4)", () => {
  async function commitRecords(storage: MemoryTransactionalStorage, key: string) {
    const loaded = await storage.load("balance", accountId(key));
    return Object.keys(loaded.metadata.commitRecords).length;
  }

  it("leaves no commit record after many transactions with a single write participant", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const a = await openAccount(network, storage, "a", clock, undefined, 0);
    const agent = new TransactionAgent(clock);

    for (let i = 0; i < 500; i++) {
      const tx = agent.startTransaction();
      await inTransaction(tx, () => a.performUpdate((s) => (s.cents += 1)));
      await agent.resolve(tx);
    }

    expect(await commitRecords(storage, "a")).toBe(0);
    expect((await storage.load("balance", accountId("a"))).committedState).toEqual({ cents: 500 });
  });

  it("leaves no commit record once every participant of many transfers has confirmed", async () => {
    const clock = new FakeTimeProvider();
    const network = new TransactionNetwork();
    const storage = new MemoryTransactionalStorage();
    const a = await openAccount(network, storage, "a", clock);
    const b = await openAccount(network, storage, "b", clock);
    const agent = new TransactionAgent(clock);

    for (let i = 0; i < 100; i++) {
      const tx = agent.startTransaction();
      await inTransaction(tx, async () => {
        await a.performUpdate((s) => (s.cents -= 1));
        await b.performUpdate((s) => (s.cents += 1));
      });
      await agent.resolve(tx);
    }
    await settle();

    expect(await commitRecords(storage, "a")).toBe(0);
    expect((await storage.load("balance", accountId("a"))).committedState).toEqual({ cents: 0 });
    expect((await storage.load("balance", accountId("b"))).committedState).toEqual({ cents: 200 });
    expect(a.blocksDeactivation).toBe(false);
    expect(b.blocksDeactivation).toBe(false);
  });
});
