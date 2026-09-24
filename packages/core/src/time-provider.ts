/** An opaque handle to a scheduled timer. */
export type TimerHandle = unknown;

/**
 * The runtime's single source of time. All timeouts and idle calculations read
 * it, so tests can substitute a deterministic fake clock.
 */
export interface TimeProvider {
  now(): number;

  /**
   * The same wall clock as {@link now}, read at the finest resolution this
   * provider offers, in nanoseconds since the Unix epoch.
   *
   * .NET's `System.TimeProvider` pairs `GetUtcNow()` with a high-resolution
   * `GetTimestamp()`/`TimestampFrequency`, and Orleans reads the fine one where
   * a millisecond is too coarse (`ActivationRebalancerWorker`). This is the same
   * split, collapsed to one reading because the epoch is what a caller minting
   * ORDERED VALUES from the clock actually wants: a millisecond is coarser than
   * the interval between commits, so values minted per-millisecond collide and
   * the sequence stops being a timestamp.
   *
   * OPTIONAL, so the two-method structural implementations that predate it — the
   * hand-rolled clocks in the transaction tests, and anything a consumer wrote
   * against the older shape — still satisfy the interface. Read it through
   * {@link nowNanosOf} rather than calling it directly, which supplies the
   * millisecond fallback for a provider that does not have it.
   */
  nowNanos?(): bigint;

  setTimer(handler: () => void, delayMs: number): TimerHandle;
  clearTimer(handle: TimerHandle): void;
}

/**
 * The whole-millisecond part of this process's clock origin, in nanoseconds.
 *
 * Read ONCE so the sub-millisecond term in {@link systemTimeProvider} is measured
 * against a fixed anchor, and scaled AFTER the conversion to `bigint`:
 * `performance.timeOrigin * 1e6` is ~1.8e18, past float64's integer precision,
 * and computing the sum as one float would quantise it back to hundreds of
 * nanoseconds.
 */
const clockOriginNanos = BigInt(Math.round(performance.timeOrigin)) * 1_000_000n;

/**
 * The largest delay `setTimeout` honours: it stores the delay in a signed
 * 32-bit field internally, so anything past `2^31 - 1` ms (~24.8 days)
 * overflows and gets clamped to 1ms instead — silently firing (near-)
 * immediately, and for a periodic re-arm, over and over.
 */
const MAX_SETTIMEOUT_MS = 0x7fffffff;

/** A `setTimer` handle that chains several native timers to reach a due time past `MAX_SETTIMEOUT_MS`. */
interface ChainedTimerHandle {
  readonly chained: true;
  clear(): void;
}

function isChainedTimerHandle(handle: TimerHandle): handle is ChainedTimerHandle {
  return (
    typeof handle === "object" &&
    handle !== null &&
    (handle as { chained?: unknown }).chained === true
  );
}

/**
 * `setTimeout`, chained past its ~24.8-day overflow point: re-arms at the
 * max supported delay, over and over, until the remaining time to `dueAt`
 * fits in one native timer. Delays at or under the limit go straight to a
 * single `setTimeout`, unchanged from before this existed.
 */
function setSystemTimer(handler: () => void, delayMs: number): TimerHandle {
  if (delayMs <= MAX_SETTIMEOUT_MS) return setTimeout(handler, delayMs);
  const dueAt = Date.now() + delayMs;
  let native: ReturnType<typeof setTimeout>;
  const armNext = (): void => {
    const remaining = dueAt - Date.now();
    native =
      remaining <= MAX_SETTIMEOUT_MS
        ? setTimeout(handler, Math.max(0, remaining))
        : setTimeout(armNext, MAX_SETTIMEOUT_MS);
  };
  armNext();
  return {
    chained: true,
    clear: () => clearTimeout(native),
  } satisfies ChainedTimerHandle;
}

export const systemTimeProvider: TimeProvider = {
  now: () => Date.now(),
  /**
   * `performance.timeOrigin + performance.now()`: the same wall clock as
   * {@link systemTimeProvider.now} with sub-microsecond resolution.
   *
   * `performance.now()` is monotonic, so this never steps backwards within the
   * process — at the cost of not absorbing wall-clock corrections made after
   * start, so it can drift from `now()`. That is the deliberate trade for a
   * caller minting ordered values: a clock that steps backwards would be worse
   * here than one that drifts.
   */
  nowNanos: () => clockOriginNanos + BigInt(Math.round(performance.now() * 1_000_000)),
  setTimer: setSystemTimer,
  clearTimer: (handle) => {
    if (isChainedTimerHandle(handle)) {
      handle.clear();
      return;
    }
    clearTimeout(handle as ReturnType<typeof setTimeout>);
  },
};

/**
 * Reads `time` at the finest resolution it offers, in nanoseconds since the Unix
 * epoch, scaling {@link TimeProvider.now} for a provider without a
 * {@link TimeProvider.nowNanos}.
 *
 * The fallback is millisecond-quantised, so a caller that needs values to be
 * distinct within a millisecond needs a provider that implements the fine
 * reading — both `systemTimeProvider` and `FakeTimeProvider` do.
 */
export function nowNanosOf(time: TimeProvider): bigint {
  return time.nowNanos?.() ?? BigInt(Math.round(time.now())) * 1_000_000n;
}
