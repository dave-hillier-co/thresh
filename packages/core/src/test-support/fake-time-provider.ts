import type { TimeProvider, TimerHandle } from "../time-provider";

interface FakeTimer {
  id: number;
  at: number;
  handler: () => void;
}

/**
 * Deterministic clock for tests. Time only moves when `advance` is called,
 * which fires any timers that come due — including timers (re)scheduled while
 * firing, so periodic work driven off `setTimer` plays out correctly.
 */
export class FakeTimeProvider implements TimeProvider {
  private current = 0;
  private nextId = 1;
  private timers: FakeTimer[] = [];

  now(): number {
    return this.current;
  }

  /**
   * The same instant as {@link now} in epoch nanoseconds, derived from the fake
   * clock rather than the machine's — so a caller that mints ordered values from
   * the high-resolution reading is still driven by {@link advance}, and reads the
   * same value twice within one fake instant.
   */
  nowNanos(): bigint {
    // Widen BEFORE scaling, for the same reason `systemTimeProvider` does: a
    // realistic epoch instant in nanoseconds (~1.8e18) is past float64's
    // integer-exact range, so `current * 1_000_000` in float rounds. Advanced
    // to 1_700_000_000_123 the float form yields ...123000064n, breaking the
    // `nowNanos() === BigInt(now()) * 1_000_000n` identity this fake promises.
    return BigInt(Math.round(this.current)) * 1_000_000n;
  }

  setTimer(handler: () => void, delayMs: number): TimerHandle {
    const timer: FakeTimer = {
      id: this.nextId++,
      at: this.current + Math.max(0, delayMs),
      handler,
    };
    this.timers.push(timer);
    return timer.id;
  }

  clearTimer(handle: TimerHandle): void {
    this.timers = this.timers.filter((t) => t.id !== handle);
  }

  advance(ms: number): void {
    const target = this.current + ms;
    for (;;) {
      const next = this.timers.filter((t) => t.at <= target).sort((a, b) => a.at - b.at)[0];
      if (next === undefined) break;
      this.timers = this.timers.filter((t) => t !== next);
      this.current = next.at;
      next.handler();
    }
    this.current = target;
  }
}
