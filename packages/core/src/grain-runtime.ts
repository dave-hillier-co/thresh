import type { Duration } from "./duration";
import type { GrainInterface } from "./grain-interface";
import type { GrainTimer } from "./grain-timer";
import type { GrainKeyFor } from "./key-kinds";
import type { StreamProvider } from "./stream";

/**
 * The runtime services a grain reaches through `this.runtime`, resolved per
 * activation.
 */
export interface GrainRuntime {
  getGrain<T>(def: GrainInterface<T>, key: GrainKeyFor<T>): T;
  registerTimer(callback: () => Promise<void>, due: Duration, period?: Duration): GrainTimer;
  registerReminder(name: string, due: Duration, period: Duration): Promise<void>;
  unregisterReminder(name: string): Promise<void>;
  getStreamProvider(name?: string): StreamProvider;
  deactivateOnIdle(): void;
  delayDeactivation(by: Duration): void;
}
