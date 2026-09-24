import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { TransactionAlreadyResolvedError } from "@thresh/core/errors";
import type { TransactionInfo } from "@thresh/core/transaction-info";
import { invocationContext } from "@thresh/runtime/invocation-context";
import { systemTimeProvider } from "@thresh/runtime/time-provider";
import { TransactionAgent } from "@thresh/runtime/transaction-agent";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { TransactionalStateImpl } from "@thresh/transactions/transactional-state-impl";

// A resource that enlists *after* its transaction's boundary has resolved it
// would never be prepared, committed or aborted — `TransactionAgent.resolve`/
// `abort` snapshot the participant set when they run, and the only points that
// release a state lock are `commit`/`abort` — so any lock it took would be held
// until the activation deactivated, with its write silently discarded. Both
// real-world triggers are ordinary shapes rather than a rare race: a `oneWay` +
// `transaction: "supported"` call whose detached callee's turn runs after the
// root resolved it, and a `Promise.all` branch that outlives the root's abort.
// `requireTransaction` (@thresh/runtime/invocation-context) refuses the
// enlistment instead, before the resource takes any lock at all.

interface Balance {
  cents: number;
}

const grainId = (key: string) => new GrainId("Account" as GrainType, key);

/** Run `fn` as if it were a turn executing inside `tx`. */
function inTransaction<R>(tx: TransactionInfo, fn: () => Promise<R>): Promise<R> {
  return invocationContext.run(
    { senderId: undefined, ownerId: undefined, reentrancyId: tx.id, transaction: tx },
    fn,
  );
}

/** A promise plus its resolver, so a test can hold a turn mid-flight. */
function deferred(): { promise: Promise<void>; resolve: () => void } {
  let resolve!: () => void;
  const promise = new Promise<void>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe("a resource enlisting after its transaction resolved", () => {
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

  it("refuses a late write after the transaction committed, leaving the resource writable", async () => {
    const state = await newState();
    const t1 = agent.startTransaction();
    await inTransaction(t1, () => state.performUpdate((s) => (s.cents = 250)));
    await agent.resolve(t1);

    // The detached callee's turn runs now, after the root already resolved —
    // the `oneWay` + `transaction: "supported"` trigger.
    await expect(
      inTransaction(t1, () => state.performUpdate((s) => (s.cents = 999))),
    ).rejects.toBeInstanceOf(TransactionAlreadyResolvedError);

    // The refused write took no lock, so the resource is still usable: without
    // the fix this write died with "wait-die: younger than a lock holder" (or
    // waited out its whole lock deadline), because a stale write lock nobody
    // would ever release was left on it.
    const t2 = agent.startTransaction();
    expect(await inTransaction(t2, () => state.performUpdate((s) => (s.cents = 300)))).toBe(300);
    await agent.resolve(t2);
    const t3 = agent.startTransaction();
    expect(await inTransaction(t3, () => state.performRead((s) => s.cents))).toBe(300);
    await agent.resolve(t3);
  });

  it("refuses a branch that outlived the root's abort, leaving the resource writable", async () => {
    // The `Promise.all` trigger: one branch dies, the root aborts, and a
    // still-running sibling branch later reaches `performUpdate`. The branch's
    // turn is already in flight here, exactly as it is in that scenario.
    const state = await newState();
    const t1 = agent.startTransaction();
    const gate = deferred();
    const slowBranch = inTransaction(t1, async () => {
      await gate.promise;
      return state.performUpdate((s) => (s.cents = 500));
    });

    await agent.abort(t1);
    gate.resolve();

    await expect(slowBranch).rejects.toBeInstanceOf(TransactionAlreadyResolvedError);

    const t2 = agent.startTransaction();
    expect(await inTransaction(t2, () => state.performUpdate((s) => (s.cents = 300)))).toBe(300);
    await agent.resolve(t2);
  });

  it("refuses a late read too, so a stale shared lock cannot wedge later writers", async () => {
    // A read enlists and takes a shared lock just as a write does, and a stale
    // shared lock is just as fatal to liveness: every younger writer dies under
    // wait-die against it, and every older one waits out its lock deadline.
    const state = await newState();
    const t1 = agent.startTransaction();
    await agent.abort(t1);

    await expect(inTransaction(t1, () => state.performRead((s) => s.cents))).rejects.toBeInstanceOf(
      TransactionAlreadyResolvedError,
    );

    // t2 is younger than t1 (the agent's clock only moves forward), so a stale
    // read lock held by t1 would kill its write instead of letting it proceed.
    const t2 = agent.startTransaction();
    expect(await inTransaction(t2, () => state.performUpdate((s) => (s.cents = 300)))).toBe(300);
    await agent.resolve(t2);
  });
});
