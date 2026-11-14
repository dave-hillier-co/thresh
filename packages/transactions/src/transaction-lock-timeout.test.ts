import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { TransactionAbortedError, TransactionBrokenLockError } from "@thresh/core/errors";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { TransactionInfo } from "@thresh/core/transaction-info";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";
import {
  DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS,
  DEFAULT_LOCK_TIMEOUT_MS,
  defaultTransactionsOptions,
  type TransactionsOptions,
} from "@thresh/transactions/transactions-options";

interface Balance {
  cents: number;
}

const grainId = (key: string) => new GrainId("Account" as GrainType, key);
const self = { grainId: grainId("a"), stateName: "balance" };

function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run({ senderId: undefined, ownerId: undefined, transaction: tx }, fn);
}

async function setUp(options: Partial<TransactionsOptions>) {
  const clock = new FakeTimeProvider();
  const storage = new MemoryTransactionalStorage();
  const state = new TransactionalStateImpl<Balance>(
    "balance",
    grainId("a"),
    () => ({ cents: 0 }),
    storage,
    undefined,
    options,
    clock,
  );
  await state.load();
  return { clock, storage, state, agent: new TransactionAgent(clock) };
}

describe("a lock holder expires (T4)", () => {
  it("defaults the hold bound and the acquire bound to Orleans' LockTimeout and LockAcquireTimeout", () => {
    expect(DEFAULT_LOCK_TIMEOUT_MS).toBe(8_000);
    expect(DEFAULT_LOCK_ACQUIRE_TIMEOUT_MS).toBe(10_000);
    expect(defaultTransactionsOptions.lockTimeoutMs).toBe(8_000);
    expect(defaultTransactionsOptions.lockAcquireTimeoutMs).toBe(10_000);
  });

  it("stops an orphaned writer blocking later transactions once the hold bound passes", async () => {
    const { clock, storage, state, agent } = await setUp({ lockTimeoutMs: 200 });
    const orphan = agent.startTransaction();
    // Its coordinator dies here: the transaction is never resolved.
    await inTransaction(orphan, () => state.performUpdate((s) => (s.cents += 1)));

    const early = agent.startTransaction();
    await expect(
      inTransaction(early, () => state.performUpdate((s) => (s.cents += 10))),
    ).rejects.toBeInstanceOf(TransactionAbortedError);

    clock.advance(200);
    const later = agent.startTransaction();
    await inTransaction(later, () => state.performUpdate((s) => (s.cents += 10)));
    await agent.resolve(later);
    expect((await storage.load("balance", grainId("a"))).committedState).toEqual({ cents: 10 });

    // The orphan's write was rolled back with its lock: a late prepare votes no,
    // and carrying on in the transaction finds the lock broken.
    expect(await state.prepare(orphan.id, { reads: 0, writes: 1 }, orphan.timeStamp, self)).toBe(
      false,
    );
    await expect(
      inTransaction(orphan, () => state.performUpdate((s) => (s.cents += 1))),
    ).rejects.toBeInstanceOf(TransactionBrokenLockError);
  });

  it("lets an older waiter proceed when the younger holder's hold bound passes", async () => {
    const { clock, state, agent } = await setUp({
      lockTimeoutMs: 200,
      lockAcquireTimeoutMs: 1_000,
    });
    const older = agent.startTransaction();
    const younger = agent.startTransaction();
    younger.timeStamp = older.timeStamp + 1;
    await inTransaction(younger, () => state.performUpdate((s) => (s.cents += 1)));

    const waiting = inTransaction(older, () => state.performUpdate((s) => (s.cents += 5)));
    clock.advance(200);
    await expect(waiting).resolves.toBe(5);
  });

  it("still bounds a waiter by the acquire bound", async () => {
    const { clock, state, agent } = await setUp({
      lockTimeoutMs: 1_000,
      lockAcquireTimeoutMs: 100,
    });
    const older = agent.startTransaction();
    const younger = agent.startTransaction();
    younger.timeStamp = older.timeStamp + 1;
    await inTransaction(younger, () => state.performUpdate((s) => (s.cents += 1)));

    const waiting = inTransaction(older, () => state.performUpdate((s) => (s.cents += 5)));
    clock.advance(100);
    await expect(waiting).rejects.toThrow(/deadline/);
  });

  it("never breaks a holder that has prepared", async () => {
    const { clock, storage, state, agent } = await setUp({ lockTimeoutMs: 200 });
    const tx = agent.startTransaction();
    await inTransaction(tx, () => state.performUpdate((s) => (s.cents += 7)));
    expect(await state.prepare(tx.id, { reads: 0, writes: 1 }, tx.timeStamp, self)).toBe(true);

    clock.advance(10_000);
    await state.recordCommit(tx.id, tx.timeStamp, [self]);
    await state.commit(tx.id);
    expect((await storage.load("balance", grainId("a"))).committedState).toEqual({ cents: 7 });
  });
});
