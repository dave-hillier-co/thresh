import { describe, expect, it } from "vitest";
import { GrainId } from "@tsva/core/grain-id";
import type { GrainType } from "@tsva/core/grain-type";
import type { TimeProvider, TimerHandle } from "@tsva/core/time-provider";
import type { TransactionInfo } from "@tsva/core/transaction-info";
import { invocationContext } from "@tsva/runtime/invocation-context";
import { systemTimeProvider } from "@tsva/runtime/time-provider";
import { TransactionAgent } from "@tsva/runtime/transaction-agent";
import {
  TransactionAbortedError,
  TransactionLockUpgradeError,
  TransactionReadOnlyViolatedError,
} from "@tsva/core/errors";
import { MemoryTransactionalStorage } from "@tsva/transactions/memory-transactional-storage";
import { TransactionalStateImpl } from "@tsva/transactions/transactional-state-impl";

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

  it("rejects a write attempted by a read-only transaction with a read-only-violation error", async () => {
    const state = await newState();
    const readOnly = agent.startTransaction(true);

    await expect(
      inTransaction(readOnly, () => state.performUpdate((s) => (s.cents = 500))),
    ).rejects.toBeInstanceOf(TransactionReadOnlyViolatedError);
    // The specific error is still a TransactionAbortedError (Orleans:
    // OrleansReadOnlyViolatedException extends OrleansTransactionAbortedException).
    await expect(
      inTransaction(readOnly, () => state.performUpdate((s) => (s.cents = 500))),
    ).rejects.toBeInstanceOf(TransactionAbortedError);

    // No lock was taken and no state was staged: a later transaction reads
    // straight through to the original committed value.
    const t2 = agent.startTransaction();
    expect(await inTransaction(t2, () => state.performRead((s) => s.cents))).toBe(100);
    await agent.resolve(t2);
  });

  it("aborts a contending transaction whose lock-acquisition deadline elapses, cascading to its participants", async () => {
    // A holds the write lock and never releases. B attempts to acquire and is
    // blocked. Because B is older than A under wait-die it would normally wait,
    // but its lock-acquisition deadline elapses, so B aborts. Every state B
    // had previously enlisted must observe abort().
    const clock = (() => {
      let current = 0;
      const timers = new Map<number, { at: number; fire: () => void }>();
      let nextId = 1;
      const fireDue = () => {
        for (const [id, t] of [...timers]) {
          if (t.at <= current) {
            timers.delete(id);
            t.fire();
          }
        }
      };
      const tp: TimeProvider & { advance(ms: number): Promise<void> } = {
        now: () => current,
        setTimer: (handler, delayMs): TimerHandle => {
          const id = nextId++;
          timers.set(id, { at: current + delayMs, fire: handler });
          return id;
        },
        clearTimer: (handle) => {
          timers.delete(handle as number);
        },
        advance: async (ms: number) => {
          current += ms;
          fireDue();
          await Promise.resolve();
        },
      };
      return tp;
    })();

    const storage = new MemoryTransactionalStorage();
    const contended = new TransactionalStateImpl<Balance>(
      "balance",
      grainId("contended"),
      () => ({ cents: 100 }),
      storage,
      undefined,
      { lockTimeoutMs: 5_000 },
      clock,
    );
    await contended.load();

    // A second state B has already enlisted in (no contention here) — the
    // cascading abort must reach this participant too.
    const other = new TransactionalStateImpl<Balance>(
      "ledger",
      grainId("other"),
      () => ({ cents: 0 }),
      new MemoryTransactionalStorage(),
      undefined,
      { lockTimeoutMs: 5_000 },
      clock,
    );
    await other.load();

    const localAgent = new TransactionAgent(clock);

    // Younger A grabs the write lock first; older B must then wait under wait-die.
    const younger = localAgent.startTransaction();
    const older = localAgent.startTransaction();
    // Make older older than younger (lower timestamp).
    (older as { timeStamp: number }).timeStamp = younger.timeStamp - 1;

    await inTransaction(younger, () => contended.performUpdate((s) => (s.cents = 1)));

    // B enlists in `other` first (uncontended) so we can observe a cascading
    // abort across multiple participants.
    await inTransaction(older, () => other.performUpdate((s) => (s.cents = 99)));

    // B attempts the contended lock — should block until the deadline.
    const attempt = inTransaction(older, () => contended.performUpdate((s) => (s.cents = 2)));

    // Drive the deadline.
    await clock.advance(5_001);

    await expect(attempt).rejects.toBeInstanceOf(TransactionAbortedError);
    await expect(attempt).rejects.toMatchObject({
      message: expect.stringContaining("deadline"),
    });

    // Caller-side: now propagate cascading abort to all enlisted participants.
    await localAgent.abort(older);

    // The `other` participant must drop B's tentative writes; reading after
    // commit shows the initial value.
    const observer = localAgent.startTransaction();
    expect(await inTransaction(observer, () => other.performRead((s) => s.cents))).toBe(0);
    await localAgent.resolve(observer);
  });

  it("a plain (non-exclusive) read-then-write race can die with a lock-upgrade error", async () => {
    // Deterministic, unit-level reproduction of the Orleans
    // ExclusiveLockTransactionTestRunner scenario, without a cluster: two
    // transactions share a read lock on the same grain state, then the
    // younger one tries to upgrade to write and — because it conflicts with
    // the older reader — dies with TransactionLockUpgradeError (Orleans
    // OrleansTransactionLockUpgradeException), exactly the failure
    // [UseExclusiveLock] exists to avoid.
    const state = await newState();
    const older = agent.startTransaction();
    const younger = agent.startTransaction();

    await inTransaction(older, () => state.performRead((s) => s.cents));
    await inTransaction(younger, () => state.performRead((s) => s.cents));

    await expect(
      inTransaction(younger, () => state.performUpdate((s) => (s.cents += 5))),
    ).rejects.toBeInstanceOf(TransactionLockUpgradeError);

    await agent.abort(younger);
    await inTransaction(older, () => state.performUpdate((s) => (s.cents += 5)));
    await agent.resolve(older);

    const check = agent.startTransaction();
    expect(await inTransaction(check, () => state.performRead((s) => s.cents))).toBe(105);
    await agent.resolve(check);
  });

  it("UseExclusiveLock (performRead exclusive:true) serializes a read-then-write instead of racing the upgrade", async () => {
    // Same shape as the previous test, but the read is marked exclusive
    // (Orleans [UseExclusiveLock]): it takes the write lock up front, so the
    // second (younger) transaction's read never shares the lock in the first
    // place — it waits/dies at the *read* step under ordinary wait-die, and
    // the surviving transaction's later write never needs to upgrade at all,
    // so no TransactionLockUpgradeError is ever raised.
    const state = await newState();
    const older = agent.startTransaction();
    const younger = agent.startTransaction();

    await inTransaction(older, () => state.performRead((s) => s.cents, { exclusive: true }));

    // The younger transaction's exclusive read now conflicts with the older
    // holder's write lock and dies under plain wait-die — never as an
    // upgrade conflict, because it never got to hold a shared read lock to
    // upgrade from.
    let youngerError: unknown;
    try {
      await inTransaction(younger, () => state.performRead((s) => s.cents, { exclusive: true }));
    } catch (err) {
      youngerError = err;
    }
    expect(youngerError).toBeInstanceOf(TransactionAbortedError);
    expect(youngerError).not.toBeInstanceOf(TransactionLockUpgradeError);

    // The older transaction's later write is a pure re-entrant hold (already
    // "write"), never an upgrade, so it always succeeds.
    await inTransaction(older, () => state.performUpdate((s) => (s.cents += 5)));
    await agent.resolve(older);
    await agent.abort(younger);

    const check = agent.startTransaction();
    expect(await inTransaction(check, () => state.performRead((s) => s.cents))).toBe(105);
    await agent.resolve(check);
  });

  /** A deterministic fake clock in the same shape the lock-deadline test above uses. */
  function fakeClock(): TimeProvider & { advance(ms: number): Promise<void> } {
    let current = 0;
    const timers = new Map<number, { at: number; fire: () => void }>();
    let nextId = 1;
    const fireDue = () => {
      for (const [id, t] of [...timers]) {
        if (t.at <= current) {
          timers.delete(id);
          t.fire();
        }
      }
    };
    return {
      now: () => current,
      setTimer: (handler, delayMs): TimerHandle => {
        const id = nextId++;
        timers.set(id, { at: current + delayMs, fire: handler });
        return id;
      },
      clearTimer: (handle) => {
        timers.delete(handle as number);
      },
      advance: async (ms: number) => {
        current += ms;
        fireDue();
        await Promise.resolve();
        await Promise.resolve();
      },
    };
  }

  it("re-resolves an in-doubt pending record via a periodic confirmation worker after the TM crashes", async () => {
    // A prepared-but-uncommitted record whose TM is a different, remote
    // participant — simulating a crash right after prepare, before commit.
    const clock = fakeClock();
    const storage = new MemoryTransactionalStorage();
    const manager = { grainId: grainId("tm"), stateName: "balance" };

    const seed = new TransactionalStateImpl<Balance>(
      "balance",
      grainId("a"),
      () => ({ cents: 100 }),
      storage,
      undefined,
      undefined,
      clock,
    );
    await seed.load();
    const tx = agent.startTransaction();
    await inTransaction(tx, () => seed.performUpdate((s) => (s.cents = 250)));
    await seed.prepare(tx.id, tx.timeStamp, manager);
    // Crash: never commits/aborts, leaving the prepared record in-doubt.

    // The reactivated facet's resolver: unreachable on the first attempt (the
    // TM's silo is still down), answers "committed" on the second.
    let calls = 0;
    const resolveStatus = async (): Promise<boolean> => {
      calls += 1;
      if (calls === 1) throw new Error("TM unreachable");
      return true;
    };

    const recovered = new TransactionalStateImpl<Balance>(
      "balance",
      grainId("a"),
      () => ({ cents: 100 }),
      storage,
      resolveStatus,
      { confirmationIntervalMs: 1_000, confirmationMaxIntervalMs: 5_000 },
      clock,
    );
    await recovered.load();

    // Recovery attempted once during load() and failed; the record is still
    // in-doubt (not yet visible as committed) and no second attempt has run yet.
    expect(calls).toBe(1);
    const beforeRetry = agent.startTransaction();
    expect(await inTransaction(beforeRetry, () => recovered.performRead((s) => s.cents))).toBe(100);
    await agent.resolve(beforeRetry);

    // Drive the confirmation worker's backoff delay: it retries and this time
    // the TM answers "committed".
    await clock.advance(1_000);
    expect(calls).toBe(2);

    const after = agent.startTransaction();
    expect(await inTransaction(after, () => recovered.performRead((s) => s.cents))).toBe(250);
    await agent.resolve(after);

    // Resolved: the worker doesn't keep firing, and a fresh activation sees no
    // pending record left to recover.
    recovered.unload();
    const reloaded = new TransactionalStateImpl<Balance>(
      "balance",
      grainId("a"),
      () => ({ cents: 100 }),
      storage,
    );
    await reloaded.load();
    const check = agent.startTransaction();
    expect(await inTransaction(check, () => reloaded.performRead((s) => s.cents))).toBe(250);
    await agent.resolve(check);
  });
});
