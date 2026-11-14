import { nowNanosOf, type TimeProvider, type TimerHandle } from "../time-provider";

/** A timer armed through a {@link PendingTimerTracker} that has neither fired nor been cleared. */
export interface PendingTimer {
  readonly delayMs: number;
  /** The frames that armed it, innermost first, without the tracker's own. */
  readonly armedAt: string;
}

/**
 * A clock wrapper that remembers every timer still pending, and where it was armed, so a test
 * can prove that nothing a stopped component armed outlives it (a pending timer is what keeps a
 * process from exiting).
 */
export class PendingTimerTracker implements TimeProvider {
  private readonly pending = new Map<TimerHandle, PendingTimer>();

  constructor(private readonly inner: TimeProvider) {}

  now(): number {
    return this.inner.now();
  }

  nowNanos(): bigint {
    return nowNanosOf(this.inner);
  }

  setTimer(handler: () => void, delayMs: number): TimerHandle {
    const armedAt = (new Error().stack ?? "").split("\n").slice(2, 8).join("\n");
    const handle = this.inner.setTimer(() => {
      this.pending.delete(handle);
      handler();
    }, delayMs);
    this.pending.set(handle, { delayMs, armedAt });
    return handle;
  }

  clearTimer(handle: TimerHandle): void {
    this.pending.delete(handle);
    this.inner.clearTimer(handle);
  }

  /** Every timer armed and neither fired nor cleared yet. */
  pendingTimers(): PendingTimer[] {
    return [...this.pending.values()];
  }
}
