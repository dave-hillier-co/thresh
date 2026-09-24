import { durationToMs, type Duration } from "@thresh/core/duration";
import type { GrainTimer } from "@thresh/core/grain-timer";
import type { TimeProvider, TimerHandle } from "@thresh/runtime/time-provider";

// Mirrors Orleans' `TimerQueueTimer.ValidateArguments` (see
// src/Orleans.Runtime/Timers/GrainTimer.cs @ v10.1.0), which in turn matches the
// bound `System.Threading.Timer.Change` enforces: once truncated to whole
// milliseconds, a due time or period must be no less than -1ms (the "infinite"
// sentinel) and no greater than 0xfffffffe ms (~49.7 days).
const MAX_SUPPORTED_TIMEOUT_MS = 0xfffffffe;
/** Orleans' `Timeout.InfiniteTimeSpan`, once truncated to milliseconds: never fire. */
const INFINITE_MS = -1;

function validateTimeoutMs(ms: number, argName: "due" | "period"): number {
  const truncated = Math.trunc(ms);
  if (truncated < -1) {
    throw new Error(
      `GrainTimer ${argName} time must not be negative (except -1 for "infinite"); got ${ms}ms`,
    );
  }
  if (truncated > MAX_SUPPORTED_TIMEOUT_MS) {
    throw new Error(
      `GrainTimer ${argName} time must not exceed ${MAX_SUPPORTED_TIMEOUT_MS}ms; got ${ms}ms`,
    );
  }
  return truncated;
}

/**
 * Per-activation timer. Fires its callback as a turn (via `runTurn`) so it never
 * races other grain methods; periodic timers are fixed-delay, arming the next
 * tick only once the previous tick's turn has settled (Orleans'
 * `GrainTimer.OnTickCompleted`). `dispose` cancels it.
 */
export class GrainTimerImpl implements GrainTimer {
  private handle: TimerHandle | undefined;
  private disposed = false;
  /** True from the native timer firing until that tick's turn settles (Orleans' `_firing`). */
  private firing = false;
  /** Whether `change()` ran since the current tick started (Orleans' `_changed`). */
  private changed = false;
  private dueMs: number;
  private periodMs: number | undefined;

  constructor(
    private readonly time: TimeProvider,
    private readonly runTurn: (callback: () => Promise<void>) => Promise<unknown>,
    private readonly callback: () => Promise<void>,
    due: Duration,
    period?: Duration,
    private readonly onError: (error: unknown) => void = () => {},
  ) {
    this.dueMs = validateTimeoutMs(durationToMs(due), "due");
    this.periodMs =
      period === undefined ? undefined : validateTimeoutMs(durationToMs(period), "period");
    this.schedule(this.dueMs);
  }

  change(due: Duration, period?: Duration): void {
    const dueMs = validateTimeoutMs(durationToMs(due), "due");
    const periodMs =
      period === undefined ? undefined : validateTimeoutMs(durationToMs(period), "period");
    if (this.disposed) return;
    this.dueMs = dueMs;
    this.periodMs = periodMs;
    this.changed = true;
    // Orleans' `GrainTimer.Change`: while a tick is firing (from the native
    // timer firing until its turn settles) the change is only recorded, and
    // `OnTickCompleted` arms the next tick from the new due time. Arming here
    // too would leave a second, untracked tick chain running alongside it.
    if (this.firing) return;
    this.clearHandle();
    this.schedule(this.dueMs);
  }

  dispose(): void {
    this.disposed = true;
    this.clearHandle();
  }

  private clearHandle(): void {
    if (this.handle !== undefined) this.time.clearTimer(this.handle);
    this.handle = undefined;
  }

  /**
   * Arm the next tick. `-1` (and an absent period) is Orleans'
   * `Timeout.InfiniteTimeSpan`: the timer stays disabled rather than handing
   * `-1` to `setTimer`, which would fire it (near-)immediately.
   */
  private schedule(delayMs: number | undefined): void {
    if (delayMs === undefined || delayMs === INFINITE_MS) return;
    this.handle = this.time.setTimer(() => this.fire(), delayMs);
  }

  private fire(): void {
    if (this.disposed) return;
    this.handle = undefined;
    this.firing = true;
    this.changed = false;
    // Mirrors Orleans' `TimerQueueTimer.TimerTick`, which catches and logs a
    // per-tick exception (scheduler admission rejection or a callback throw)
    // rather than letting it propagate — an unhandled rejection here would
    // otherwise crash the process (Node's default) and, for a periodic timer,
    // there'd be nothing left to log it since `fire` isn't awaited by anyone.
    //
    // Fixed-delay (Orleans `GrainTimer.OnTickCompleted`, ~GrainTimer.cs:138-167):
    // the next period is armed only once THIS tick's turn has settled, never
    // before. Rearming up front (fixed-rate) let a slow callback's ticks queue
    // without bound — risking `MaxEnqueuedRequestsHardLimit` — or, for an
    // interleaving timer, run several ticks of the same timer concurrently.
    let turn: Promise<unknown>;
    try {
      turn = this.runTurn(this.callback);
    } catch (error) {
      // A synchronous admission throw must still settle the tick, or `firing`
      // would stay set and the timer would never re-arm.
      turn = Promise.reject(error);
    }
    turn
      .catch((error) => this.onError(error))
      .finally(() => {
        // `OnTickCompleted`: a change made during the tick re-arms from its
        // new due time; otherwise the next tick is one period away.
        this.firing = false;
        if (this.disposed) return;
        this.schedule(this.changed ? this.dueMs : this.periodMs);
      });
  }
}
