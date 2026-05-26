import { TransactionAbortedError } from "@tsva/core/errors";

type LockMode = "read" | "write";

interface Holder {
  priority: number;
  mode: LockMode;
}

interface Waiter {
  transactionId: string;
  priority: number;
  mode: LockMode;
  resolve: () => void;
}

/**
 * A timestamp-ordered reader-writer lock with **wait-die** deadlock avoidance,
 * ported from Orleans' `Orleans.Transactions/State/ReaderWriterLock.cs`. A
 * transaction's lower timestamp means older / higher priority. Reads share;
 * a write is exclusive. On a conflict the **older** requester waits and the
 * **younger** one dies (aborts) — because the wound direction follows the total
 * timestamp order, no wait cycle can form. A transaction holds the lock from its
 * first access until it commits or aborts (across turns), which is what keeps a
 * second transaction from observing tentative state.
 */
export class ReaderWriterLock {
  private readonly holders = new Map<string, Holder>();
  private readonly waiters: Waiter[] = [];

  /**
   * Acquire — or re-enter / upgrade — the lock for a transaction. Resolves once
   * granted; rejects with {@link TransactionAbortedError} if the transaction must
   * die under wait-die.
   */
  enter(transactionId: string, priority: number, mode: LockMode): Promise<void> {
    const held = this.holders.get(transactionId);
    if (held !== undefined) {
      // Re-entrant. Upgrade read -> write if no other holder conflicts.
      if (mode === "write" && held.mode === "read") {
        if (this.conflictingHolders(transactionId, "write").length > 0) {
          return this.blockOrDie(transactionId, priority, "write");
        }
        held.mode = "write";
      }
      return Promise.resolve();
    }
    if (this.conflictingHolders(transactionId, mode).length === 0) {
      this.holders.set(transactionId, { priority, mode });
      return Promise.resolve();
    }
    return this.blockOrDie(transactionId, priority, mode);
  }

  /** Release a transaction's hold (and any queued request), then grant waiters. */
  release(transactionId: string): void {
    this.holders.delete(transactionId);
    for (let i = this.waiters.length - 1; i >= 0; i--) {
      if (this.waiters[i]!.transactionId === transactionId) this.waiters.splice(i, 1);
    }
    this.pump();
  }

  private blockOrDie(transactionId: string, priority: number, mode: LockMode): Promise<void> {
    const conflicts = this.conflictingHolders(transactionId, mode);
    // Wait-die: wait only if older (strictly lower timestamp) than every
    // conflicting holder; otherwise die.
    const olderThanAll = conflicts.every((h) => priority < h.priority);
    if (!olderThanAll) {
      return Promise.reject(
        new TransactionAbortedError(transactionId, "wait-die: younger than a lock holder"),
      );
    }
    return new Promise<void>((resolve) => {
      this.waiters.push({ transactionId, priority, mode, resolve });
    });
  }

  /** Grant any waiters that no longer conflict, oldest (lowest timestamp) first. */
  private pump(): void {
    let progressed = true;
    while (progressed) {
      progressed = false;
      const queued = [...this.waiters].sort((a, b) => a.priority - b.priority);
      for (const waiter of queued) {
        if (this.conflictingHolders(waiter.transactionId, waiter.mode).length === 0) {
          this.holders.set(waiter.transactionId, {
            priority: waiter.priority,
            mode: waiter.mode,
          });
          this.waiters.splice(this.waiters.indexOf(waiter), 1);
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
