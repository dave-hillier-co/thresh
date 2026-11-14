import { describe, expect, it } from "vitest";
import { TransactionAbortedError, TransactionBrokenLockError } from "@thresh/core/errors";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import {
  account,
  asRemote,
  dispatcherOver,
  inTransaction,
  openAccount,
  settle,
  TransactionNetwork,
} from "@thresh/transactions/test-support/transaction-network";

async function setUp() {
  const clock = new FakeTimeProvider();
  const network = new TransactionNetwork();
  const storage = new MemoryTransactionalStorage();
  const a = await openAccount(network, storage, "a", clock);
  const b = await openAccount(network, storage, "b", clock);
  const agent = new TransactionAgent(clock);
  agent.setDispatcher(dispatcherOver(network));
  return { clock, network, storage, a, b, agent };
}

describe("a read-only transaction commits in one phase (Orleans CommitReadOnly)", () => {
  it("commits with commitReadOnly and nothing else", async () => {
    const { network, a, b, agent } = await setUp();
    const tx = agent.startTransaction(true);
    await inTransaction(tx, async () => {
      await a.performRead((s) => s.cents);
      await b.performRead((s) => s.cents);
    });
    asRemote(tx, account("a"));
    asRemote(tx, account("b"));

    await agent.resolve(tx);

    expect(network.sent.map((m) => m.method)).toEqual(["commitReadOnly", "commitReadOnly"]);
    expect(a.blocksDeactivation).toBe(false);
    expect(b.blocksDeactivation).toBe(false);
  });

  it("aborts when a participant's lock was broken", async () => {
    const { network, storage, b, agent, clock } = await setUp();
    const tx = agent.startTransaction(true);
    await inTransaction(tx, () => b.performRead((s) => s.cents));
    asRemote(tx, account("b"));
    b.unload();
    await openAccount(network, storage, "b", clock);

    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionBrokenLockError);
    expect(network.messages("abort", account("b")).length).toBe(1);
  });

  it("aborts when a participant's recorded access does not match its lock", async () => {
    const { network, a, agent } = await setUp();
    const tx = agent.startTransaction(true);
    await inTransaction(tx, () => a.performRead((s) => s.cents));
    asRemote(tx, account("a"));
    // The coordinator recorded a read that never reached the lock.
    tx.participants.get([...tx.participants.keys()][0]!)!.access.reads += 1;

    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionBrokenLockError);
    await settle();
    // Rolled back: a writer is not blocked by it.
    const later = agent.startTransaction();
    await inTransaction(later, () => a.performUpdate((s) => (s.cents += 1)));
    await agent.resolve(later);
    expect(network.messages("commitReadOnly").map((m) => m.target)).toHaveLength(1);
  });
});

describe("a read-only participant of a read-write transaction (Orleans: prepared, not committed read-only)", () => {
  it("is prepared like any participant, votes, and needs no confirmation", async () => {
    const { network, storage, a, b, agent } = await setUp();
    const tx = agent.startTransaction();
    await inTransaction(tx, async () => {
      await a.performUpdate((s) => (s.cents += 1));
      await b.performRead((s) => s.cents);
    });
    asRemote(tx, account("b"));

    await agent.resolve(tx);
    await settle();

    expect(network.messages("prepare", account("b")).length).toBe(1);
    expect(network.messages("prepared", account("a")).map((m) => m.args[3])).toEqual(["ok"]);
    expect(network.messages("confirm")).toEqual([]);
    expect(network.messages("commitReadOnly")).toEqual([]);
    expect((await storage.load("balance", account("a").grainId)).metadata.commitRecords).toEqual(
      {},
    );
    expect(b.blocksDeactivation).toBe(false);
  });

  it("aborts the transaction when its lock was broken", async () => {
    const { network, storage, a, b, agent, clock } = await setUp();
    const tx = agent.startTransaction();
    await inTransaction(tx, async () => {
      await a.performUpdate((s) => (s.cents += 1));
      await b.performRead((s) => s.cents);
    });
    asRemote(tx, account("b"));
    b.unload();
    await openAccount(network, storage, "b", clock);

    await expect(agent.resolve(tx)).rejects.toBeInstanceOf(TransactionAbortedError);
    expect(network.messages("prepared", account("a")).map((m) => m.args[3])).toEqual([
      "brokenLock",
    ]);
    expect((await storage.load("balance", account("a").grainId)).committedSequenceId).toBe(0);
  });
});
