// Ported from dotnet/orleans test/Grains/TestInternalGrains/TimerGrain.cs @ v10.1.0 (MIT)
// (INonReentrantTimerCallGrain / NonReentrantTimerCallGrain section).
import type { Duration } from "@tsva/core/duration";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { GrainTimer } from "@tsva/core/grain-timer";
import { INonReentrantTimerCallGrain } from "@tsva/parity/grains/interfaces/timer-grain-interfaces";

export { INonReentrantTimerCallGrain };

// Not @reentrant(): the grain defaults to non-reentrant, so timer ticks and
// `externalTick` calls serialize with each other exactly like upstream's
// non-reentrant grain.
@grain({ name: "UnitTests.Grains.NonReentrantTimerCallGrain" })
export class NonReentrantTimerCallGrain extends Grain implements INonReentrantTimerCallGrain {
  private tickCount = 0;
  private lastException: string | undefined;
  private timers = new Map<string, GrainTimer>();

  async getTickCount(): Promise<number> {
    return this.tickCount;
  }

  async getException(): Promise<string | undefined> {
    return this.lastException;
  }

  async startTimer(name: string, delay: Duration): Promise<void> {
    const timer = this.runtime.registerTimer(async () => {
      try {
        this.tickCount++;
      } catch (error) {
        this.lastException = error instanceof Error ? error.message : String(error);
      }
    }, delay);
    this.timers.set(name, timer);
  }

  async stopTimer(name: string): Promise<void> {
    this.timers.get(name)?.dispose();
    this.timers.delete(name);
  }

  async externalTick(_name: string): Promise<void> {
    this.tickCount++;
  }
}
