// Ported from dotnet/orleans test/Orleans.Core.Tests/Async_AsyncExecutorWithRetriesTests.cs @ v10.1.0 (MIT).
import { describe, expect, it } from "vitest";
import {
  executeWithRetries,
  fixedBackoff,
  INFINITE_RETRIES,
} from "@thresh/core/async-executor-with-retries";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";

/**
 * Pumps the microtask queue and advances `time` in lockstep until `promise`
 * settles. Needed because `executeWithRetries`'s backoff delay suspends on a
 * `FakeTimeProvider` timer between attempts rather than a real one.
 */
async function settleWithBackoff(time: FakeTimeProvider, promise: Promise<unknown>): Promise<void> {
  let settled = false;
  promise.then(
    () => (settled = true),
    () => (settled = true),
  );
  for (let i = 0; i < 20 && !settled; i++) {
    await Promise.resolve();
    await Promise.resolve();
    time.advance(1_000);
  }
}

describe("NonSilo.Tests.Async_AsyncExecutorWithRetriesTests", () => {
  // NonSilo.Tests.Async_AsyncExecutorWithRetriesTests.Async_AsyncExecutorWithRetriesTest_1
  it("retries a failing function until it succeeds, and fails when max retries are exceeded", async () => {
    let counter = 0;
    const myFunc = async (funcCounter: number): Promise<number> => {
      expect(funcCounter).toBe(counter);
      counter++;
      if (counter === 5) return 28;
      throw new Error("Wrong arg!");
    };
    const errorFilter = () => true;

    const value = await executeWithRetries(myFunc, {
      maxRetries: 10,
      shouldRetry: (attempt, outcome) => outcome.kind === "error" && errorFilter(),
    });
    expect(value).toBe(28);

    counter = 0;
    await expect(
      executeWithRetries(myFunc, {
        maxRetries: 3,
        shouldRetry: (attempt, outcome) => outcome.kind === "error" && errorFilter(),
      }),
    ).rejects.toThrow();
  });

  // NonSilo.Tests.Async_AsyncExecutorWithRetriesTests.Async_AsyncExecutorWithRetriesTest_2
  it("retries on success until the success filter is satisfied", async () => {
    let counter = 0;
    const countLimit = 5;
    const myFunc = async (funcCounter: number): Promise<number> => {
      expect(funcCounter).toBe(counter);
      return ++counter;
    };
    const successFilter = (count: number) => count !== countLimit;

    let maxRetries = 10;
    let expectedRetries = countLimit;
    let value = await executeWithRetries(myFunc, {
      maxRetries,
      shouldRetry: (attempt, outcome) => outcome.kind === "success" && successFilter(outcome.value),
    });
    expect(value).toBe(expectedRetries);
    expect(counter).toBe(value);

    counter = 0;
    maxRetries = 3;
    expectedRetries = maxRetries;
    value = await executeWithRetries(myFunc, {
      maxRetries,
      shouldRetry: (attempt, outcome) => outcome.kind === "success" && successFilter(outcome.value),
    });
    expect(value).toBe(expectedRetries);
    expect(counter).toBe(value);
  });

  // NonSilo.Tests.Async_AsyncExecutorWithRetriesTests.Async_AsyncExecutorWithRetriesTest_4
  it("does not invoke the error filter when the function succeeds on the first attempt", async () => {
    let counter = 0;
    let lastIteration = 0;
    const myFunc = async (funcCounter: number): Promise<number> => {
      lastIteration = funcCounter;
      expect(funcCounter).toBe(counter);
      return ++counter;
    };
    const errorFilter = (_error: unknown, i: number) => {
      expect(i).toBe(lastIteration);
      throw new Error("Should not be called");
    };

    const maxRetries = 5;
    const time = new FakeTimeProvider();
    const promise = executeWithRetries(myFunc, {
      maxRetries,
      shouldRetry: (attempt, outcome) =>
        outcome.kind === "error" ? errorFilter(outcome.error, attempt) : false,
      backoff: fixedBackoff(1_000),
      timeProvider: time,
    });
    await settleWithBackoff(time, promise);
    const value = await promise;

    expect(value).toBe(counter);
    expect(counter).toBe(1);
  });

  // NonSilo.Tests.Async_AsyncExecutorWithRetriesTests.Async_AsyncExecutorWithRetriesTest_5
  it("respects the error filter's decision to retry, stop, or rethrow", async () => {
    let counter = 0;
    let lastIteration = 0;
    const myFunc = async (funcCounter: number): Promise<number> => {
      lastIteration = funcCounter;
      expect(funcCounter).toBe(counter);
      counter++;
      throw new Error(String(counter));
    };
    const errorFilter = (exc: unknown, i: number): boolean => {
      expect(i).toBe(lastIteration);
      if (i === 0 || i === 1) return true;
      if (i === 2) throw exc;
      return false;
    };

    const maxRetries = 5;
    const time = new FakeTimeProvider();
    const promise = executeWithRetries(myFunc, {
      maxRetries,
      shouldRetry: (attempt, outcome) =>
        outcome.kind === "error" ? errorFilter(outcome.error, attempt) : false,
      backoff: fixedBackoff(1_000),
      timeProvider: time,
    });
    await settleWithBackoff(time, promise);

    await expect(promise).rejects.toThrow("3");
    expect(counter).toBe(3);
  });

  it("supports INFINITE_RETRIES to retry indefinitely until the predicate declines", async () => {
    let counter = 0;
    const value = await executeWithRetries(
      async () => {
        counter++;
        if (counter < 4) throw new Error("not yet");
        return counter;
      },
      {
        maxRetries: INFINITE_RETRIES,
        shouldRetry: (_attempt, outcome) => outcome.kind === "error",
      },
    );
    expect(value).toBe(4);
  });
});
