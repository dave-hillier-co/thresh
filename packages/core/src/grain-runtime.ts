import type { Duration } from "./duration";
import type { GrainInterface } from "./grain-interface";
import type { GrainKeyFor } from "./key-kinds";

/**
 * The runtime services a grain reaches through `this.runtime`, resolved per
 * activation. Phase-1 subset; persistence, timers, reminders and streams are
 * added in later phases.
 */
export interface GrainRuntime {
  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T;
  deactivateOnIdle(): void;
  delayDeactivation(by: Duration): void;
}
