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

/** Options for `registerTimer` (Orleans' `GrainTimerCreationOptions`). */
export interface TimerOptions {
  /**
   * Let the timer's callback interleave with a running non-reentrant call
   * (Orleans' `GrainTimerCreationOptions.Interleave`). Defaults to `false` — a
   * timer turn is serialized with the grain's other turns like any call. Set it
   * when a callback must run while an outer call awaits it (e.g. a
   * self-disposing timer), which would otherwise deadlock a non-reentrant grain.
   */
  interleave?: boolean;
  /**
   * Treat each tick as activity that keeps the activation alive for idle
   * collection, the same as an ordinary grain call (Orleans'
   * `GrainTimerCreationOptions.KeepAlive`, via the tick message's
   * `IsKeepAlive` — see `ActivationData.OnCompletedRequest`,
   * ActivationData.cs:1486). Defaults to `false`: a timer's own ticks do NOT
   * by themselves postpone collection, matching this class's default doc
   * above and Orleans' own `KeepAlive` default.
   */
  keepAlive?: boolean;
}
