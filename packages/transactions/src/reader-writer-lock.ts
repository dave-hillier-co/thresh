import {
  TransactionAbortedError,
  TransactionBrokenLockError,
  TransactionLockUpgradeError,
} from "@thresh/core/errors";
import type { AccessCounter } from "@thresh/core/transaction-info";
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
  /** Reads granted to this transaction while it held the lock (Orleans `TransactionRecord.NumberReads`). */
  reads: number;
  /** Writes granted to this transaction while it held the lock (Orleans `TransactionRecord.NumberWrites`). */
  writes: number;
  /**
   * Prepared: it has left the lock group for the commit queue (Orleans
   * `LockExits`), so the hold bound no longer applies to it. It still blocks
   * conflicting transactions until it commits or aborts.
   */
  exited: boolean;
}

/** Construction options for {@link ReaderWriterLock}. */
export interface ReaderWriterLockOptions {
  /**
   * Orleans `TransactionalStateOptions.LockTimeout`: the longest the current
   * holders may keep the lock, in milliseconds, measured from the last time a
   * transaction joined them. Absent means holders never expire.
   */
  readonly lockTimeoutMs?: number | undefined;
  /**
   * Called for each holder whose lock the hold bound broke, after it has been
   * removed, so the resource can roll back its tentative state (Orleans
   * `BreakLock` notifying the queue).
   */
  readonly onBreak?: ((transactionId: string) => void) | undefined;
}

/**
 * The outcome of {@link ReaderWriterLock.validate}, named after the Orleans
 * `TransactionalStatus` values `ReaderWriterLock.ValidateLock` returns.
 */
export type LockValidation = "ok" | "brokenLock" | "lockValidationFailed";

/**
 * What one {@link ReaderWriterLock.enter} is for: whether it counts as a read
 * or a write (an exclusive read takes a write lock but counts as a read, as in
 * Orleans), and the access the caller's transaction has already recorded on
 * this resource — so a transaction that should already hold the lock and does
 * not is refused (Orleans `EnterLock`'s broken-lock checks).
 */
export interface LockAccess {
  readonly isRead: boolean;
  readonly counter?: AccessCounter | undefined;
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
  isRead: boolean;
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
 * This mirrors Orleans' `TransactionalStateOptions.LockAcquireTimeout`.
 *
 * Holders are bounded too ({@link ReaderWriterLockOptions.lockTimeoutMs},
 * Orleans `LockTimeout`): the current holders form Orleans' lock group, whose
 * deadline is set when a transaction joins it. When it passes, every holder
 * that has not prepared ({@link exit}) is broken — removed, its queued requests
 * rejected and the resource told through `onBreak` — and waiters are granted.
 * The broken transaction finds out at its next access
 * ({@link TransactionBrokenLockError}) or when its prepare fails validation.
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
  private holdTimer: TimerHandle | undefined;
  private disposed = false;

  constructor(
    private readonly time: TimeProvider = systemTimeProvider,
    private readonly options: ReaderWriterLockOptions = {},
  ) {}

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
    access?: LockAccess,
  ): Promise<void> {
    const isRead = access?.isRead ?? mode === "read";
    const counter = access?.counter;
    const held = this.holders.get(transactionId);
    if (held !== undefined) {
      // Orleans `EnterLock`: re-entering with more recorded access than this
      // lock granted means some of it was lost (the lock was broken and taken
      // again from scratch).
      if (counter !== undefined && (counter.reads > held.reads || counter.writes > held.writes)) {
        return Promise.reject(
          new TransactionBrokenLockError(transactionId, "when re-entering lock"),
        );
      }
      // Re-entrant. Upgrade read -> write if no other holder conflicts.
      if (mode === "write" && held.mode === "read") {
        if (this.conflictingHolders(transactionId, "write").length > 0) {
          return this.blockOrDie(
            transactionId,
            priority,
            "write",
            isRead,
            deadline,
            /* isUpgrade */ true,
          );
        }
        held.mode = "write";
      }
      this.count(held, isRead);
      return Promise.resolve();
    }
    // Orleans `EnterLock`: a transaction that already read or wrote here is
    // supposed to hold this lock; finding no record of it means the lock was
    // broken, and taking it afresh would silently drop that earlier access.
    if (counter !== undefined && counter.reads + counter.writes > 0) {
      return Promise.reject(
        new TransactionBrokenLockError(transactionId, "when trying to re-enter lock"),
      );
    }
    if (this.conflictingHolders(transactionId, mode).length === 0) {
      this.grant(transactionId, priority, mode, isRead);
      return Promise.resolve();
    }
    return this.blockOrDie(transactionId, priority, mode, isRead, deadline);
  }

  /**
   * Whether `transactionId` still holds this lock with exactly `accessCount`
   * (Orleans `ReaderWriterLock.ValidateLock`): `brokenLock` when it holds no
   * lock here at all, `lockValidationFailed` when the reads or writes the
   * coordinator recorded differ from those this lock granted. Unlike Orleans
   * this does not roll the transaction back itself; the resource does, since it
   * owns the tentative state.
   */
  validate(transactionId: string, accessCount: AccessCounter): LockValidation {
    const held = this.holders.get(transactionId);
    if (held === undefined) return "brokenLock";
    if (held.reads !== accessCount.reads || held.writes !== accessCount.writes) {
      return "lockValidationFailed";
    }
    return "ok";
  }

  /**
   * The resource is going away: cancel the hold bound and every waiter's
   * acquisition deadline so no timer fires into a dead resource (or keeps the
   * event loop alive), and settle queued requests as aborted. Holders are left
   * as they are; the lock is not used again.
   */
  dispose(): void {
    this.disposed = true;
    this.clearHoldTimer();
    for (const waiter of this.waiters.splice(0)) {
      if (waiter.timer !== undefined) this.time.clearTimer(waiter.timer);
      waiter.reject(
        new TransactionAbortedError(waiter.transactionId, "transactional state unloaded"),
      );
    }
  }

  /** Whether any transaction currently holds this lock. */
  get isHeld(): boolean {
    return this.holders.size > 0;
  }

  /**
   * `transactionId` has prepared: it leaves the lock group for the commit
   * queue (Orleans `LockExits` → `EnqueueCommit`), so the hold bound no longer
   * applies to it — breaking a prepared participant's lock would lose a write
   * the TM may already have committed. It keeps blocking conflicting
   * transactions until it is released.
   */
  exit(transactionId: string): void {
    const held = this.holders.get(transactionId);
    if (held === undefined) return;
    held.exited = true;
    if (!this.hasUnexitedHolder()) this.clearHoldTimer();
  }

  private grant(transactionId: string, priority: number, mode: LockMode, isRead: boolean): void {
    const existing = this.holders.get(transactionId);
    if (existing !== undefined) {
      // A queued read-to-write upgrade: keep the reads already counted.
      existing.mode = mode;
      this.count(existing, isRead);
      return;
    }
    const holder: Holder = {
      transactionId,
      priority,
      mode,
      reads: 0,
      writes: 0,
      exited: false,
    };
    this.count(holder, isRead);
    this.holders.set(transactionId, holder);
    this.armHoldTimer();
  }

  /**
   * (Re)start the hold bound: Orleans sets the lock group's deadline to now +
   * `LockTimeout` whenever a transaction joins the group holding the lock.
   */
  private armHoldTimer(): void {
    const timeoutMs = this.options.lockTimeoutMs;
    if (timeoutMs === undefined || this.disposed) return;
    this.clearHoldTimer();
    this.holdTimer = this.time.setTimer(() => {
      this.holdTimer = undefined;
      this.breakExpiredHolders();
    }, timeoutMs);
  }

  private clearHoldTimer(): void {
    if (this.holdTimer === undefined) return;
    this.time.clearTimer(this.holdTimer);
    this.holdTimer = undefined;
  }

  private hasUnexitedHolder(): boolean {
    for (const holder of this.holders.values()) if (!holder.exited) return true;
    return false;
  }

  /**
   * The lock group timed out (Orleans `LockWork` → `AbortExecutingTransactions`):
   * break every holder that has not prepared, then let waiters in.
   */
  private breakExpiredHolders(): void {
    const broken = [...this.holders.values()].filter((h) => !h.exited);
    for (const holder of broken) {
      this.holders.delete(holder.transactionId);
      this.rejectQueued(holder.transactionId, "lock broken: hold timeout exceeded");
    }
    for (const holder of broken) this.options.onBreak?.(holder.transactionId);
    this.pump();
  }

  private count(holder: Holder, isRead: boolean): void {
    if (isRead) holder.reads += 1;
    else holder.writes += 1;
  }

  /** Release a transaction's hold (and any queued request), then grant waiters. */
  release(transactionId: string): void {
    this.holders.delete(transactionId);
    if (!this.hasUnexitedHolder()) this.clearHoldTimer();
    this.rejectQueued(transactionId, "lock released while waiting");
    this.pump();
  }

  /** Settle and drop every queued request of `transactionId`. */
  private rejectQueued(transactionId: string, reason: string): void {
    for (let i = this.waiters.length - 1; i >= 0; i--) {
      const waiter = this.waiters[i]!;
      if (waiter.transactionId === transactionId) {
        if (waiter.timer !== undefined) this.time.clearTimer(waiter.timer);
        this.waiters.splice(i, 1);
        // Settle the orphaned promise, exactly as the deadline path does. A
        // released transaction's own queued request can never be granted now —
        // there is nothing left to grant it on behalf of — and merely splicing
        // the entry out leaves the awaiting `performUpdate` suspended forever:
        // the grain's exclusive turn never completes, so the activation stops
        // serving calls entirely, not just for that transaction. Reachable
        // whenever a transaction holds a read lock and is queued for an
        // upgrade on the same resource and is then aborted elsewhere (a dying
        // sibling participant, a caller deadline, a rejected `Promise.all`) —
        // the same abort path that leads here.
        waiter.reject(new TransactionAbortedError(transactionId, reason));
      }
    }
  }

  private blockOrDie(
    transactionId: string,
    priority: number,
    mode: LockMode,
    isRead: boolean,
    deadline?: AcquireDeadline,
    isUpgrade = false,
  ): Promise<void> {
    if (this.disposed) {
      return Promise.reject(
        new TransactionAbortedError(transactionId, "transactional state unloaded"),
      );
    }
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
      const waiter: Waiter = { transactionId, priority, mode, isRead, resolve, reject };
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
          this.grant(waiter.transactionId, waiter.priority, waiter.mode, waiter.isRead);
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
