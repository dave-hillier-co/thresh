// Ported from dotnet/orleans src/Orleans.Core/Async/AsyncExecutorWithRetries.cs @ v10.1.0 (MIT).
import type { TimeProvider } from "./time-provider";
import { systemTimeProvider } from "./time-provider";

/** Pass as `maxRetries` to allow retrying indefinitely, until `shouldRetry` declines. */
export const INFINITE_RETRIES = -1;

/** What happened on one attempt: either it resolved with a value, or it threw. */
export type RetryOutcome<T> =
  | { readonly kind: "success"; readonly value: T }
  | { readonly kind: "error"; readonly error: unknown };

/**
 * Decides whether to try again after an attempt. Called with the 0-based
 * attempt number that just ran and its outcome. Invoked on both success and
 * failure — returning `false` on success stops with that value; returning
 * `true` on failure retries the failing call.
 */
export type ShouldRetry<T> = (attempt: number, outcome: RetryOutcome<T>) => boolean;

/** Returns the delay (in ms) before the next attempt, given the attempt just made. */
export type BackoffProvider = (attempt: number) => number | undefined;

/** A backoff provider that always waits the same fixed delay. */
export function fixedBackoff(delayMs: number): BackoffProvider {
  return () => delayMs;
}

export interface ExecuteWithRetriesOptions<T> {
  /** Maximum number of attempts, or `INFINITE_RETRIES` for no cap. */
  readonly maxRetries: number;
  readonly shouldRetry: ShouldRetry<T>;
  readonly backoff?: BackoffProvider;
  readonly timeProvider?: TimeProvider;
}

/**
 * Runs `action` with retries: on each attempt's outcome (success or thrown
 * error), asks `shouldRetry` whether to try again, subject to `maxRetries`.
 * Stops and settles with the current outcome once `shouldRetry` declines (or
 * the retry budget is exhausted) — resolving on a settled success, rethrowing
 * on a settled error. If `shouldRetry` itself throws, that exception
 * propagates immediately without being treated as a retry decision.
 */
export async function executeWithRetries<T>(
  action: (attempt: number) => Promise<T>,
  options: ExecuteWithRetriesOptions<T>,
): Promise<T> {
  const { maxRetries, shouldRetry, backoff, timeProvider = systemTimeProvider } = options;
  let attempt = 0;

  for (;;) {
    let outcome: RetryOutcome<T>;
    try {
      outcome = { kind: "success", value: await action(attempt) };
    } catch (error) {
      outcome = { kind: "error", error };
    }

    const callsMade = attempt + 1;
    const withinBudget = maxRetries === INFINITE_RETRIES || callsMade < maxRetries;
    const retry = withinBudget && shouldRetry(attempt, outcome);

    if (!retry) {
      if (outcome.kind === "error") throw outcome.error;
      return outcome.value;
    }

    const delayMs = backoff?.(attempt);
    attempt++;
    if (delayMs !== undefined && delayMs > 0) {
      await sleep(timeProvider, delayMs);
    }
  }
}

function sleep(timeProvider: TimeProvider, delayMs: number): Promise<void> {
  return new Promise((resolve) => {
    timeProvider.setTimer(resolve, delayMs);
  });
}
