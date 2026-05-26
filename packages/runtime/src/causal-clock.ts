import type { TimeProvider } from "@tsva/runtime/time-provider";

/**
 * Monotonic logical clock for transaction timestamps, ported from Orleans'
 * `Orleans.Transactions/Utilities/CausalClock.cs`. Each reading is strictly
 * greater than the previous one (and at least the wall clock), so every
 * transaction gets a unique, totally ordered timestamp that doubles as its
 * priority for wait-die. `merge` folds in a timestamp observed from another
 * silo so causally related transactions stay ordered across the cluster.
 */
export class CausalClock {
  private previous = 0;

  constructor(private readonly time: TimeProvider) {}

  /** A fresh timestamp strictly greater than any previously issued or merged. */
  utcNow(): number {
    this.previous = Math.max(this.previous + 1, this.time.now());
    return this.previous;
  }

  /** Fold in an observed timestamp without necessarily advancing past now. */
  merge(timestamp: number): number {
    this.previous = Math.max(this.previous, timestamp);
    return this.previous;
  }

  /** Fold in an observed timestamp and issue a fresh reading past it and now. */
  mergeUtcNow(timestamp: number): number {
    this.previous = Math.max(Math.max(this.previous + 1, timestamp + 1), this.time.now());
    return this.previous;
  }
}
