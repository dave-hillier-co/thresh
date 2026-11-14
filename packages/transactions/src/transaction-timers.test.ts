import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { TransactionInfo } from "@thresh/core/transaction-info";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { TransactionTimerTracker } from "@thresh/transactions/test-support/transaction-timer-tracker";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";

const grainId = new GrainId("Account" as GrainType, "a");

function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run({ senderId: undefined, ownerId: undefined, transaction: tx }, fn);
}

describe("no timer outlives the transactional state that armed it (T4)", () => {
  it("leaves no pending timer after unloading a state whose lock is held", async () => {
    const clock = new TransactionTimerTracker(new FakeTimeProvider());
    const state = new TransactionalStateImpl<{ cents: number }>(
      "balance",
      grainId,
      () => ({ cents: 0 }),
      new MemoryTransactionalStorage(),
      undefined,
      { lockTimeoutMs: 8_000 },
      clock,
    );
    await state.load();
    const tx = new TransactionAgent(clock).startTransaction();
    await inTransaction(tx, () => state.performUpdate((s) => (s.cents += 1)));
    // The hold bound is armed while the lock is held.
    expect(clock.pendingTransactionTimers()).not.toEqual([]);

    state.unload();

    expect(clock.pendingTransactionTimers()).toEqual([]);
  });

  it("does not call back into an unloaded state when the hold bound would have passed", async () => {
    const fake = new FakeTimeProvider();
    const storage = new MemoryTransactionalStorage();
    const state = new TransactionalStateImpl<{ cents: number }>(
      "balance",
      grainId,
      () => ({ cents: 0 }),
      storage,
      undefined,
      { lockTimeoutMs: 100 },
      fake,
    );
    await state.load();
    const tx = new TransactionAgent(fake).startTransaction();
    await inTransaction(tx, () => state.performUpdate((s) => (s.cents += 1)));
    state.unload();

    fake.advance(1_000);

    // Had the timer fired, the holder would have been broken and this prepare
    // would vote no through the break path; instead the lock simply remains.
    expect(state.blocksDeactivation).toBe(true);
  });
});
