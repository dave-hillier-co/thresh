import { durationToMs, type Duration } from "@tsva/core/duration";
import type { GrainTimer } from "@tsva/core/grain-timer";
import type { TimeProvider, TimerHandle } from "@tsva/runtime/time-provider";

// Mirrors Orleans' `TimerQueueTimer.ValidateArguments` (see
// src/Orleans.Runtime/Timers/GrainTimer.cs @ v10.1.0), which in turn matches the
// bound `System.Threading.Timer.Change` enforces: once truncated to whole
// milliseconds, a due time or period must be no less than -1ms (the "infinite"
// sentinel) and no greater than 0xfffffffe ms (~49.7 days).
const MAX_SUPPORTED_TIMEOUT_MS = 0xfffffffe;

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
 * races other grain methods; periodic timers reschedule at a fixed rate before
 * running the callback. `dispose` cancels it.
 */
export class GrainTimerImpl implements GrainTimer {
  private handle: TimerHandle | undefined;
  private disposed = false;
  private dueMs: number;
  private periodMs: number | undefined;

  constructor(
    private readonly time: TimeProvider,
    private readonly runTurn: (callback: () => Promise<void>) => Promise<unknown>,
    private readonly callback: () => Promise<void>,
    due: Duration,
    period?: Duration,
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
    if (this.handle !== undefined) this.time.clearTimer(this.handle);
    this.dueMs = dueMs;
    this.periodMs = periodMs;
    this.schedule(this.dueMs);
  }

  dispose(): void {
    this.disposed = true;
    if (this.handle !== undefined) this.time.clearTimer(this.handle);
    this.handle = undefined;
  }

  private schedule(delayMs: number): void {
    this.handle = this.time.setTimer(() => this.fire(), delayMs);
  }

  private fire(): void {
    if (this.disposed) return;
    // Reschedule first (fixed-rate) so periodic ticks don't drift with turn time.
    if (this.periodMs !== undefined) this.schedule(this.periodMs);
    void this.runTurn(this.callback);
  }
}
