// Ported from dotnet/orleans test/Grains/TestInternalGrains/TimerGrain.cs @ v10.1.0 (MIT)
// (ITimerCallGrain / TimerCallGrain section).
import type { Duration } from "@thresh/core/duration";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { GrainTimer } from "@thresh/core/grain-timer";
import { ITimerCallGrain } from "@thresh/parity/grains/interfaces/timer-grain-interfaces";

export { ITimerCallGrain };

@grain({ name: "UnitTests.Grains.TimerCallGrain" })
export class TimerCallGrain extends Grain implements ITimerCallGrain {
  private tickCount = 0;
  private lastException: string | undefined;
  private timers = new Map<string, GrainTimer>();

  async getTickCount(): Promise<number> {
    return this.tickCount;
  }

  async getException(): Promise<string | undefined> {
    return this.lastException;
  }

  async startTimer(name: string, dueTime: Duration, operationType?: string): Promise<void> {
    this.register(name, dueTime, undefined, operationType);
  }

  async restartTimer(name: string, dueTime: Duration, period?: Duration): Promise<void> {
    this.timers.get(name)?.dispose();
    this.register(name, dueTime, period);
  }

  async stopTimer(name: string): Promise<void> {
    this.timers.get(name)?.dispose();
    this.timers.delete(name);
  }

  async runSelfDisposingTimer(): Promise<void> {
    await new Promise<void>((resolve) => {
      // Interleave so this callback can run while the outer call awaits it on a
      // non-reentrant grain (Orleans' GrainTimerCreationOptions.Interleave).
      const timer = this.runtime.registerTimer(
        async () => {
          timer.dispose();
          resolve();
        },
        period0,
        undefined,
        { interleave: true },
      );
      this.timers.set("self-disposing", timer);
    });
  }

  private register(
    name: string,
    dueTime: Duration,
    period: Duration | undefined,
    operationType?: string,
  ): void {
    // `timer` is referenced by its own callback (update_period/dispose_timer
    // below); safe despite the TDZ because the callback only runs on a later
    // turn, once `registerTimer` has returned and `timer` is initialized.
    const timer: GrainTimer = this.runtime.registerTimer(
      async () => {
        try {
          this.tickCount++;
          if (operationType === "update_period") {
            timer.change(period100, period100);
          } else if (operationType === "dispose_timer") {
            timer.dispose();
            this.timers.delete(name);
          }
        } catch (error) {
          this.lastException = error instanceof Error ? error.message : String(error);
        }
      },
      dueTime,
      period,
      { interleave: true },
    );
    this.timers.set(name, timer);
  }
}

const period0: Duration = { ms: 0 };
const period100: Duration = { seconds: 100 };
