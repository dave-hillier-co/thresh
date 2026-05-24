import { durationToMs, type Duration } from "@tsva/core/duration";
import type { GrainTimer } from "@tsva/core/grain-timer";
import type { TimeProvider, TimerHandle } from "@tsva/runtime/time-provider";

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
    this.dueMs = durationToMs(due);
    this.periodMs = period === undefined ? undefined : durationToMs(period);
    this.schedule(this.dueMs);
  }

  change(due: Duration, period?: Duration): void {
    if (this.disposed) return;
    if (this.handle !== undefined) this.time.clearTimer(this.handle);
    this.dueMs = durationToMs(due);
    this.periodMs = period === undefined ? undefined : durationToMs(period);
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
