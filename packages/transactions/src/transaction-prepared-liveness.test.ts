import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { TransactionAbortedError } from "@thresh/core/errors";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { ParticipantId, TransactionInfo } from "@thresh/core/transaction-info";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";

interface Balance {
  cents: number;
}

const grainId = (key: string) => new GrainId("Account" as GrainType, key);
const participant = (key: string): ParticipantId => ({
  grainId: grainId(key),
  stateName: "balance",
});
const settle = () => new Promise<void>((resolve) => setImmediate(resolve));

function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run({ senderId: undefined, ownerId: undefined, transaction: tx }, fn);
}

/** A TM `a` and a participant `b`, both prepared, whose coordinator then dies. */
async function orphanedAfterPrepare(remoteTransactionPingFrequencyMs = 60_000) {
  const clock = new FakeTimeProvider();
  const storage = new MemoryTransactionalStorage();
  const options = { prepareTimeoutMs: 20_000, remoteTransactionPingFrequencyMs };
  const a = new TransactionalStateImpl<Balance>(
    "balance",
    grainId("a"),
    () => ({ cents: 100 }),
    storage,
    undefined,
    options,
    clock,
  );
  await a.load();
  const b = new TransactionalStateImpl<Balance>(
    "balance",
    grainId("b"),
    () => ({ cents: 100 }),
    storage,
    async (_manager, transactionId) => a.status(transactionId),
    options,
    clock,
  );
  await b.load();
  const agent = new TransactionAgent(clock);
  const tx = agent.startTransaction();
  await inTransaction(tx, () => a.performUpdate((s) => (s.cents -= 30)));
  await inTransaction(tx, () => b.performUpdate((s) => (s.cents += 30)));
  const manager = participant("a");
  expect(await a.prepare(tx.id, { reads: 0, writes: 1 }, tx.timeStamp, manager)).toBe(true);
  expect(await b.prepare(tx.id, { reads: 0, writes: 1 }, tx.timeStamp, manager)).toBe(true);
  return { clock, storage, a, b, agent, tx, manager };
}

async function committed(storage: MemoryTransactionalStorage, key: string) {
  const loaded = await storage.load("balance", grainId(key));
  return {
    cents: loaded.committedSequenceId === 0 ? 100 : (loaded.committedState as Balance).cents,
    pending: loaded.pendingStates.length,
  };
}

describe("a prepared transaction whose coordinator dies is still resolved", () => {
  it("a TM that prepared decides abort after PrepareTimeout, and its participants follow", async () => {
    const { clock, storage, a, b, agent, tx, manager } = await orphanedAfterPrepare();

    clock.advance(20_000);
    await settle();
    expect(a.status(tx.id)).toBe("presumedAbort");
    expect(a.blocksDeactivation).toBe(false);
    await expect(a.recordCommit(tx.id, tx.timeStamp, [manager])).rejects.toBeInstanceOf(
      TransactionAbortedError,
    );

    // The participant asks at its ping interval, learns the abort, and lets go.
    expect(b.blocksDeactivation).toBe(true);
    clock.advance(40_000);
    await settle();
    expect(b.blocksDeactivation).toBe(false);
    expect(await committed(storage, "a")).toEqual({ cents: 100, pending: 0 });
    expect(await committed(storage, "b")).toEqual({ cents: 100, pending: 0 });

    // Both locks are free again.
    const later = agent.startTransaction();
    await inTransaction(later, () => a.performUpdate((s) => (s.cents += 1)));
    await inTransaction(later, () => b.performUpdate((s) => (s.cents += 1)));
    await agent.resolve(later);
  });

  it("a participant applies a commit the TM recorded before the coordinator died", async () => {
    const { clock, storage, a, tx, manager } = await orphanedAfterPrepare();
    await a.recordCommit(tx.id, tx.timeStamp, [manager, participant("b")]);
    await a.commit(tx.id);

    clock.advance(60_000);
    await settle();
    expect(await committed(storage, "b")).toEqual({ cents: 130, pending: 0 });
  });

  it("a TM that recorded the commit but never applied it applies it at PrepareTimeout", async () => {
    const { clock, storage, a, tx, manager } = await orphanedAfterPrepare();
    await a.recordCommit(tx.id, tx.timeStamp, [manager, participant("b")]);

    clock.advance(20_000);
    await settle();
    expect(await committed(storage, "a")).toEqual({ cents: 70, pending: 0 });
  });

  it("a participant keeps its record while the TM is undetermined, and asks again", async () => {
    const { clock, storage, b } = await orphanedAfterPrepare(5_000);
    // The TM is still deciding: its own prepare timeout is longer than the ping.
    clock.advance(5_000);
    await settle();
    expect(b.blocksDeactivation).toBe(true);
    expect(await committed(storage, "b")).toEqual({ cents: 100, pending: 1 });

    // The TM decides abort at 20 s; the next ping after that learns it.
    for (let elapsed = 5_000; elapsed < 25_000; elapsed += 5_000) {
      clock.advance(5_000);
      await settle();
    }
    expect(b.blocksDeactivation).toBe(false);
    expect(await committed(storage, "b")).toEqual({ cents: 100, pending: 0 });
  });
});
