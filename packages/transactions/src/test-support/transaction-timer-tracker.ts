import { nowNanosOf, type TimeProvider, type TimerHandle } from "@thresh/core/time-provider";

/**
 * A clock wrapper that remembers every timer still pending which was armed from
 * inside the transactions package (judged by the creating call stack), so a test
 * can prove no timer outlives the resource that armed it.
 */
export class TransactionTimerTracker implements TimeProvider {
  private readonly pending = new Map<TimerHandle, string>();

  constructor(private readonly inner: TimeProvider) {}

  now(): number {
    return this.inner.now();
  }

  nowNanos(): bigint {
    return nowNanosOf(this.inner);
  }

  setTimer(handler: () => void, delayMs: number): TimerHandle {
    const stack = new Error().stack ?? "";
    const fromTransactions = /packages\/transactions\/src\/(?!test-support)[^\s:)]*\.ts/.test(
      stack.split("\n").slice(2).join("\n"),
    );
    const handle = this.inner.setTimer(() => {
      this.pending.delete(handle);
      handler();
    }, delayMs);
    if (fromTransactions) this.pending.set(handle, stack.split("\n")[2]?.trim() ?? "");
    return handle;
  }

  clearTimer(handle: TimerHandle): void {
    this.pending.delete(handle);
    this.inner.clearTimer(handle);
  }

  /** Where each still-pending transactions-package timer was armed. */
  pendingTransactionTimers(): string[] {
    return [...this.pending.values()];
  }
}
