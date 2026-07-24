import { GrainCallAbortedError } from "./errors";

/**
 * Combine zero or more possibly-`undefined` `AbortSignal`s into one signal
 * that fires when any of them does — `undefined` when none were given, the
 * single signal unwrapped when exactly one was, and `AbortSignal.any` over
 * the rest. Used to compose ambient cancellation sources (an explicit
 * `InvokeCallOptions.signal`, a per-call deadline, a bound
 * `GrainCancellationToken`) into the one signal a turn observes.
 */
export function combineSignals(
  signals: readonly (AbortSignal | undefined)[],
): AbortSignal | undefined {
  const present = signals.filter((s): s is AbortSignal => s !== undefined);
  if (present.length === 0) return undefined;
  if (present.length === 1) return present[0];
  return AbortSignal.any(present);
}

/**
 * Race `promise` against `signal`: if the signal fires first, reject with a
 * {@link GrainCallAbortedError} without waiting for `promise` to settle. The
 * underlying operation is NOT interrupted (e.g. a Postgres query already sent
 * to the server keeps running server-side) — this only abandons the *wait*
 * for it, the same abandon-without-interrupting shape as
 * `GrainFactory.raceResponseDeadline`'s caller-side response timeout.
 * `signal` absent, or a `promise` that settles first, passes the result
 * through unchanged.
 */
export function raceSignal<T>(promise: Promise<T>, signal: AbortSignal | undefined): Promise<T> {
  if (signal === undefined) return promise;
  if (signal.aborted) return Promise.reject(new GrainCallAbortedError());
  return new Promise<T>((resolve, reject) => {
    const onAbort = (): void => reject(new GrainCallAbortedError());
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (err: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(err);
      },
    );
  });
}
