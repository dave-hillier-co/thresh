import type { Duration } from "./duration";
import type { GrainInterface } from "./grain-interface";
import type { GrainTimer } from "./grain-timer";
import type { GrainKeyFor } from "./key-kinds";

/**
 * The runtime services a grain reaches through `this.runtime`, resolved per
 * activation. Persistence, reminders and streams are added in later phases.
 */
export interface GrainRuntime {
  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T;
  registerTimer(callback: () => Promise<void>, due: Duration, period?: Duration): GrainTimer;
  deactivateOnIdle(): void;
  delayDeactivation(by: Duration): void;
}
