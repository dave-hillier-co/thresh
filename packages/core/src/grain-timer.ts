import type { Duration } from "./duration";

/**
 * A non-durable, per-activation timer. Its callback fires as a turn (so it
 * respects single-threaded execution) and it is cancelled when the activation
 * deactivates. A timer does not keep a grain alive by itself.
 */
export interface GrainTimer {
  change(due: Duration, period?: Duration): void;
  dispose(): void;
}
