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

/**
 * The value {@link raceAbort} resolves to when the signal fires before the promise settles.
 * A unique symbol, so it can never collide with a legitimate result value.
 */
export const ABORTED: unique symbol = Symbol("thresh.aborted");

/**
 * Race `promise` against `signal`, resolving to {@link ABORTED} if the signal fires first
 * rather than rejecting — the settle-with-a-sentinel counterpart of {@link raceSignal}.
 *
 * Use it where cancellation is a *clean exit* rather than an error: the shape a C# port writes
 * as `Task.WhenAny(work, Task.Delay(Timeout.Infinite, ct))`, where the cancellation ends a loop
 * normally. Reaching for `raceSignal` there turns a `yield break` into a thrown
 * `GrainCallAbortedError`, which is a behaviour change, not a translation.
 *
 * As with `raceSignal`, the underlying operation is NOT interrupted — this abandons only the
 * *wait* for it. A rejection from `promise` itself still propagates.
 */
export function raceAbort<T>(
  promise: Promise<T>,
  signal: AbortSignal | undefined,
): Promise<T | typeof ABORTED> {
  if (signal === undefined) return promise;
  if (signal.aborted) return Promise.resolve(ABORTED);
  return new Promise<T | typeof ABORTED>((resolve, reject) => {
    const onAbort = (): void => resolve(ABORTED);
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
