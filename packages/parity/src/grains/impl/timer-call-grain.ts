// Ported from dotnet/orleans test/Grains/TestInternalGrains/TimerGrain.cs @ v10.1.0 (MIT)
// (ITimerCallGrain / TimerCallGrain section).
// `operationType` is accepted for interface fidelity but not interpreted: the only
// ported test that varies it (GrainTimer_Change, exercising timer.Change() validation
// and callback-initiated Change/dispose) is GAP-TIMER-VALIDATION — this framework's
// GrainTimer.change() performs no due/period range validation, so that test cannot be
// faithfully ported yet.
import type { Duration } from "@tsva/core/duration";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { GrainTimer } from "@tsva/core/grain-timer";
import { ITimerCallGrain } from "@tsva/parity/grains/interfaces/timer-grain-interfaces";

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

  async startTimer(name: string, dueTime: Duration): Promise<void> {
    this.register(name, dueTime, undefined);
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
      const timer = this.runtime.registerTimer(async () => {
        timer.dispose();
        resolve();
      }, period0);
      this.timers.set("self-disposing", timer);
    });
  }

  private register(name: string, dueTime: Duration, period: Duration | undefined): void {
    const timer = this.runtime.registerTimer(
      async () => {
        try {
          this.tickCount++;
        } catch (error) {
          this.lastException = error instanceof Error ? error.message : String(error);
        }
      },
      dueTime,
      period,
    );
    this.timers.set(name, timer);
  }
}

const period0: Duration = { ms: 0 };
