/** An opaque handle to a scheduled timer. */
export type TimerHandle = unknown;

/**
 * The runtime's single source of time. All timeouts and idle calculations read
 * it, so tests can substitute a deterministic fake clock.
 */
export interface TimeProvider {
  now(): number;
  setTimer(handler: () => void, delayMs: number): TimerHandle;
  clearTimer(handle: TimerHandle): void;
}

export const systemTimeProvider: TimeProvider = {
  now: () => Date.now(),
  setTimer: (handler, delayMs) => setTimeout(handler, delayMs),
  clearTimer: (handle) => clearTimeout(handle as ReturnType<typeof setTimeout>),
};
