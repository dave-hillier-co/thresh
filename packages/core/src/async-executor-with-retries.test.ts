import { describe, expect, it } from "vitest";
import {
  executeWithRetries,
  fixedBackoff,
  INFINITE_RETRIES,
  type RetryOutcome,
} from "./async-executor-with-retries";
import { FakeTimeProvider } from "./test-support/fake-time-provider";

describe("executeWithRetries", () => {
  it("retries a failing action until it succeeds", async () => {
    let counter = 0;
    const value = await executeWithRetries(
      async (attempt) => {
        expect(attempt).toBe(counter);
        counter++;
        if (counter === 5) return 28;
        throw new Error("not yet");
      },
      { maxRetries: 10, shouldRetry: (_attempt, outcome) => outcome.kind === "error" },
    );

    expect(value).toBe(28);
    expect(counter).toBe(5);
  });

  it("stops retrying and rethrows once the retry budget is exhausted", async () => {
    let counter = 0;
    await expect(
      executeWithRetries(
        async () => {
          counter++;
          throw new Error("always fails");
        },
        { maxRetries: 3, shouldRetry: () => true },
      ),
    ).rejects.toThrow("always fails");

    expect(counter).toBe(3);
  });

  it("retries on success too, until the predicate is satisfied", async () => {
    let counter = 0;
    const countLimit = 5;

    const value = await executeWithRetries(async () => ++counter, {
      maxRetries: INFINITE_RETRIES,
      shouldRetry: (_attempt, outcome) =>
        outcome.kind === "success" ? outcome.value !== countLimit : true,
    });

    expect(value).toBe(countLimit);
    expect(counter).toBe(countLimit);
  });

  it("does not call the predicate at all when the first attempt succeeds and it declines", async () => {
    let counter = 0;
    let predicateCalls = 0;

    const value = await executeWithRetries(async () => ++counter, {
      maxRetries: 5,
      shouldRetry: () => {
        predicateCalls++;
        return false;
      },
    });

    expect(value).toBe(1);
    expect(counter).toBe(1);
    expect(predicateCalls).toBe(1);
  });

  it("propagates an exception thrown by the predicate itself, without treating it as a retry decision", async () => {
    let counter = 0;

    await expect(
      executeWithRetries(
        async () => {
          counter++;
          throw new Error(`attempt ${counter}`);
        },
        {
          maxRetries: 5,
          shouldRetry: (attempt, outcome: RetryOutcome<never>) => {
            if (attempt === 2)
              throw outcome.kind === "error" ? outcome.error : new Error("unexpected");
            return true;
          },
        },
      ),
    ).rejects.toThrow("attempt 3");

    expect(counter).toBe(3);
  });

  it("waits the backoff delay between attempts, driven by the injected TimeProvider", async () => {
    const time = new FakeTimeProvider();
    let counter = 0;
    const delaysObserved: number[] = [];
    let settled = false;

    const promise = executeWithRetries(
      async () => {
        counter++;
        delaysObserved.push(time.now());
        if (counter < 3) throw new Error("retry me");
        return counter;
      },
      {
        maxRetries: 5,
        shouldRetry: (_attempt, outcome) => outcome.kind === "error",
        backoff: fixedBackoff(1_000),
        timeProvider: time,
      },
    );
    promise.then(
      () => (settled = true),
      () => (settled = true),
    );

    // Nothing to await on directly (the executor is asleep between attempts), so pump the
    // microtask queue and advance the fake clock in lockstep until the promise settles.
    for (let i = 0; i < 20 && !settled; i++) {
      await Promise.resolve();
      await Promise.resolve();
      time.advance(1_000);
    }

    const value = await promise;
    expect(value).toBe(3);
    expect(counter).toBe(3);
    expect(delaysObserved).toEqual([0, 1_000, 2_000]);
  });
});
