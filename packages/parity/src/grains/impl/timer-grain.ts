// Ported from dotnet/orleans test/Grains/TestInternalGrains/TimerGrain.cs @ v10.1.0 (MIT).
// Trimmed to the members the ported DefaultCluster timer tests exercise; upstream's
// StartTimer/SetCounter/LongWait on ITimerGrain aren't reached by any ported test.
import type { Duration } from "@thresh/core/duration";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import type { GrainTimer } from "@thresh/core/grain-timer";
import { ITimerGrain } from "@thresh/parity/grains/interfaces/timer-grain-interfaces";

export { ITimerGrain };

const period: Duration = { ms: 100 };

@grain({ name: "UnitTests.Grains.TimerGrain" })
export class TimerGrain extends Grain implements ITimerGrain {
  private counter = 0;
  private defaultTimer: GrainTimer | undefined;

  override async onActivate(): Promise<void> {
    this.counter = 0;
    this.defaultTimer = this.runtime.registerTimer(async () => void this.counter++, period, period);
  }

  async stopDefaultTimer(): Promise<void> {
    this.defaultTimer?.dispose();
  }

  async getTimerPeriod(): Promise<Duration> {
    return period;
  }

  async getCounter(): Promise<number> {
    return this.counter;
  }

  async deactivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }
}
