import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import type { GrainType } from "@tsva/core/grain-type";
import type { TransactionInfo } from "@tsva/core/transaction-info";
import { invocationContext } from "@tsva/runtime/invocation-context";
import { systemTimeProvider } from "@tsva/runtime/time-provider";
import { TransactionAgent } from "@tsva/runtime/transaction-agent";
import { TransactionAbortedError } from "@tsva/core/errors";
import { MemoryTransactionalStorage } from "@tsva/transactions/memory-transactional-storage";
import { TransactionalStateImpl } from "@tsva/transactions/transactional-state-impl";

interface Balance {
  cents: number;
}

const grainId = (key: string) => new GrainId("Account" as GrainType, key);

/** Run `fn` as if it were a turn executing inside `tx`. */
function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run({ senderId: undefined, reentrancyId: tx.id, transaction: tx }, fn);
}

describe("TransactionalStateImpl (Slice 2)", () => {
  const agent = new TransactionAgent(systemTimeProvider);
  const newState = async (): Promise<TransactionalStateImpl<Balance>> => {
    const state = new TransactionalStateImpl<Balance>(
      "balance",
      grainId("a"),
      () => ({ cents: 100 }),
      new MemoryTransactionalStorage(),
    );
    await state.load();
    return state;
  };

  it("applies a tentative write to the committed version only on commit", async () => {
    const state = await newState();
    const t1 = agent.startTransaction();
    await inTransaction(t1, () => state.performUpdate((s) => (s.cents = 250)));

    // The write is tentative until t1 commits; a fresh transaction then sees it.
    // (A concurrent reader cannot observe the committed value meanwhile — the
    // write lock makes the resource serializable, exercised separately below.)
    await agent.resolve(t1);
    const t2 = agent.startTransaction();
    expect(await inTransaction(t2, () => state.performRead((s) => s.cents))).toBe(250);
    await agent.resolve(t2);
  });

  it("discards tentative writes on abort", async () => {
    const state = await newState();
    const t1 = agent.startTransaction();
    await inTransaction(t1, () => state.performUpdate((s) => (s.cents = 999)));
    await agent.abort(t1);

    const t2 = agent.startTransaction();
    const value = await inTransaction(t2, () => state.performRead((s) => s.cents));
    expect(value).toBe(100);
  });

  it("a transaction sees its own tentative writes within the transaction", async () => {
    const state = await newState();
    const t1 = agent.startTransaction();
    const seen = await inTransaction(t1, async () => {
      await state.performUpdate((s) => (s.cents += 5));
      return state.performRead((s) => s.cents);
    });
    expect(seen).toBe(105);
    await agent.resolve(t1);
  });

  it("serializes contending writers by timestamp: the younger one dies", async () => {
    const state = await newState();
    const older = agent.startTransaction(); // earlier timestamp
    const younger = agent.startTransaction(); // later timestamp

    // Older writer acquires and holds the write lock (no commit yet).
    await inTransaction(older, () => state.performUpdate((s) => (s.cents = 1)));

    // Younger writer conflicts with the older holder and must die (wait-die).
    await expect(
      inTransaction(younger, () => state.performUpdate((s) => (s.cents = 2))),
    ).rejects.toBeInstanceOf(TransactionAbortedError);

    await agent.resolve(older);
    const t = agent.startTransaction();
    expect(await inTransaction(t, () => state.performRead((s) => s.cents))).toBe(1);
  });
});
