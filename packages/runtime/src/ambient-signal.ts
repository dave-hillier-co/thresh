import type { TimeProvider } from "@tsva/core/time-provider";

/**
 * An `AbortSignal` that fires once `deadlineMs` (absolute epoch ms) passes,
 * scheduled against `time` rather than the wall clock so a fake clock
 * (`FakeTimeProvider`) drives it deterministically in tests — the same shape
 * as `GrainFactory.raceResponseDeadline`'s caller-side response timeout.
 * Already-passed deadlines abort synchronously. `dispose()` clears the
 * pending timer once it's no longer needed (the turn already settled), so a
 * call that finishes before its deadline doesn't leak one.
 */
export function deadlineSignal(
  deadlineMs: number,
  time: TimeProvider,
): { signal: AbortSignal; dispose(): void } {
  const controller = new AbortController();
  const remaining = deadlineMs - time.now();
  if (remaining <= 0) {
    controller.abort();
    return { signal: controller.signal, dispose(): void {} };
  }
  const handle = time.setTimer(() => controller.abort(), remaining);
  return {
    signal: controller.signal,
    dispose(): void {
      time.clearTimer(handle);
    },
  };
}
