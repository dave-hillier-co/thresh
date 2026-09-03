import { TransactionAbortedError, TransactionLockUpgradeError } from "@thresh/core/errors";
import {
  systemTimeProvider,
  type TimeProvider,
  type TimerHandle,
} from "@thresh/core/time-provider";

type LockMode = "read" | "write";

interface Holder {
  transactionId: string;
  priority: number;
  mode: LockMode;
}

/**
 * True if `a` is strictly older (higher wait-die priority) than `b`. Lower
 * timestamp wins; a tie — possible across silos, since {@link CausalClock}
 * is only per-silo monotonic — is broken by lexicographic transaction id so
 * the order is total. Both silos see the same id string, so they agree on
 * the winner regardless of which side evaluates the comparison first.
 */
function isOlder(
  a: { priority: number; transactionId: string },
  b: { priority: number; transactionId: string },
): boolean {
  if (a.priority !== b.priority) return a.priority < b.priority;
  return a.transactionId < b.transactionId;
}

interface Waiter {
  transactionId: string;
  priority: number;
  mode: LockMode;
  resolve: () => void;
  reject: (err: Error) => void;
  timer?: TimerHandle;
}

/** Bound on how long {@link ReaderWriterLock.enter} may wait to be granted. */
export interface AcquireDeadline {
  /** Relative wait budget in milliseconds; the lock samples the clock to compute the absolute deadline. */
  timeoutMs: number;
}

/**
 * A timestamp-ordered reader-writer lock with **wait-die** deadlock avoidance,
 * ported from Orleans' `Orleans.Transactions/State/ReaderWriterLock.cs`. A
 * transaction's lower timestamp means older / higher priority; ties (possible
 * across silos, since `CausalClock` is only per-silo monotonic) break on
 * lexicographic transaction id, so priority is really the pair `(timestamp,
 * id)` compared lexicographically — a total order both silos agree on, since
 * both see the same id string regardless of which side evaluates first.
 * Reads share; a write is exclusive. On a conflict the **older** requester
 * waits and the **younger** one dies (aborts) — because the wound direction
 * follows the total order, no wait cycle can form and, unlike a strict
 * timestamp-only comparison, an exact tie no longer makes both sides die. A
 * transaction holds the lock from its first access until it commits or
 * aborts (across turns), which is what keeps a second transaction from
 * observing tentative state.
 *
 * Callers may pass an {@link AcquireDeadline} to bound how long a wait-die
 * waiter blocks; when the deadline elapses the waiter is removed and the
 * promise rejects with {@link TransactionAbortedError} (`deadline exceeded`),
 * which the caller propagates as a cascading abort to its other participants.
 * This mirrors Orleans' `TransactionalStateOptions.LockTimeout`.
 *
 * A holder re-entering with `"write"` while it only holds `"read"` upgrades
 * in place if no conflict, and otherwise follows the same wait-die rule —
 * except a death on that specific read-to-write upgrade path rejects with the
 * more specific {@link TransactionLockUpgradeError} (Orleans
 * `OrleansTransactionLockUpgradeException`) rather than the generic
 * {@link TransactionAbortedError}.
 */
export class ReaderWriterLock {
  private readonly holders = new Map<string, Holder>();
  private readonly waiters: Waiter[] = [];

  constructor(private readonly time: TimeProvider = systemTimeProvider) {}

  /**
   * Acquire — or re-enter / upgrade — the lock for a transaction. Resolves once
   * granted; rejects with {@link TransactionAbortedError} if the transaction must
   * die under wait-die or its acquisition `deadline` elapses first.
   */
  enter(
    transactionId: string,
    priority: number,
    mode: LockMode,
    deadline?: AcquireDeadline,
  ): Promise<void> {
    const held = this.holders.get(transactionId);
    if (held !== undefined) {
      // Re-entrant. Upgrade read -> write if no other holder conflicts.
      if (mode === "write" && held.mode === "read") {
        if (this.conflictingHolders(transactionId, "write").length > 0) {
          return this.blockOrDie(transactionId, priority, "write", deadline, /* isUpgrade */ true);
        }
        held.mode = "write";
      }
      return Promise.resolve();
    }
    if (this.conflictingHolders(transactionId, mode).length === 0) {
      this.holders.set(transactionId, { transactionId, priority, mode });
      return Promise.resolve();
    }
    return this.blockOrDie(transactionId, priority, mode, deadline);
  }

  /** Release a transaction's hold (and any queued request), then grant waiters. */
  release(transactionId: string): void {
    this.holders.delete(transactionId);
    for (let i = this.waiters.length - 1; i >= 0; i--) {
      const waiter = this.waiters[i]!;
      if (waiter.transactionId === transactionId) {
        if (waiter.timer !== undefined) this.time.clearTimer(waiter.timer);
        this.waiters.splice(i, 1);
      }
    }
    this.pump();
  }

  private blockOrDie(
    transactionId: string,
    priority: number,
    mode: LockMode,
    deadline?: AcquireDeadline,
    isUpgrade = false,
  ): Promise<void> {
    const conflicts = this.conflictingHolders(transactionId, mode);
    // Wait-die: wait only if older — by the total (timestamp, id) order —
    // than every conflicting holder; otherwise die.
    const requester = { priority, transactionId };
    const olderThanAll = conflicts.every((h) => isOlder(requester, h));
    if (!olderThanAll) {
      // A read-to-write upgrade that dies gets the upgrade-specific typed
      // error (Orleans OrleansTransactionLockUpgradeException); an ordinary
      // first-acquisition death keeps the generic wait-die reason.
      return Promise.reject(
        isUpgrade
          ? new TransactionLockUpgradeError(transactionId)
          : new TransactionAbortedError(transactionId, "wait-die: younger than a lock holder"),
      );
    }
    return new Promise<void>((resolve, reject) => {
      const waiter: Waiter = { transactionId, priority, mode, resolve, reject };
      if (deadline !== undefined) {
        waiter.timer = this.time.setTimer(() => {
          // Time out only if still waiting; if the waiter was already granted
          // or released, its entry is gone and we no-op.
          const idx = this.waiters.indexOf(waiter);
          if (idx === -1) return;
          this.waiters.splice(idx, 1);
          reject(new TransactionAbortedError(transactionId, "lock acquisition deadline exceeded"));
        }, deadline.timeoutMs);
      }
      this.waiters.push(waiter);
    });
  }

  /** Grant any waiters that no longer conflict, oldest (lowest timestamp) first. */
  private pump(): void {
    let progressed = true;
    while (progressed) {
      progressed = false;
      const queued = [...this.waiters].sort((a, b) => (isOlder(a, b) ? -1 : isOlder(b, a) ? 1 : 0));
      for (const waiter of queued) {
        if (this.conflictingHolders(waiter.transactionId, waiter.mode).length === 0) {
          this.holders.set(waiter.transactionId, {
            transactionId: waiter.transactionId,
            priority: waiter.priority,
            mode: waiter.mode,
          });
          this.waiters.splice(this.waiters.indexOf(waiter), 1);
          if (waiter.timer !== undefined) this.time.clearTimer(waiter.timer);
          waiter.resolve();
          progressed = true;
        }
      }
    }
  }

  private conflictingHolders(transactionId: string, mode: LockMode): Holder[] {
    const conflicts: Holder[] = [];
    for (const [id, holder] of this.holders) {
      if (id === transactionId) continue;
      if (mode === "write" || holder.mode === "write") conflicts.push(holder);
    }
    return conflicts;
  }
}
