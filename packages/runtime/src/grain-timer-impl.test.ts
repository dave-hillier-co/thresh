import { describe, expect, it } from "vitest";
import type { Duration } from "@thresh/core/duration";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { GrainTimerImpl } from "@thresh/runtime/grain-timer-impl";

// Mirrors Orleans' `TimerQueueTimer.ValidateArguments` (see
// src/Orleans.Runtime/Timers/GrainTimer.cs @ v10.1.0): due time and period,
// once converted to milliseconds, must be >= -1 and <= 0xfffffffe (the same
// bound `System.Threading.Timer` enforces).
const MAX_SUPPORTED_TIMEOUT_MS = 0xfffffffe;

function newTimer(time: FakeTimeProvider, due: Duration = { ms: 1000 }) {
  return new GrainTimerImpl(
    time,
    (cb) => cb(),
    async () => {},
    due,
  );
}

const flush = () => new Promise((r) => setTimeout(r, 0));

describe("GrainTimerImpl", () => {
  it("accepts zero and sub-millisecond due times and periods", () => {
    const time = new FakeTimeProvider();
    const timer = newTimer(time);

    expect(() => timer.change({ ms: 0 })).not.toThrow();
    expect(() => timer.change({ ms: 0.01 })).not.toThrow();
    expect(() => timer.change({ ms: 0 }, { ms: 0 })).not.toThrow();
    expect(() => timer.change({ ms: 0.01 }, { ms: 0.01 })).not.toThrow();
    // Truncating toward zero (as the (long) cast in Orleans does) rounds
    // small negative sub-ms values up to 0, which is still valid.
    expect(() => timer.change({ ms: -0.4 })).not.toThrow();
    expect(() => timer.change({ seconds: 1 }, { ms: -0.5 })).not.toThrow();

    timer.dispose();
  });

  it("accepts -1ms (the infinite-timeout sentinel)", () => {
    const time = new FakeTimeProvider();
    const timer = newTimer(time);

    expect(() => timer.change({ ms: -1 })).not.toThrow();
    expect(() => timer.change({ ms: 1000 }, { ms: -1 })).not.toThrow();

    timer.dispose();
  });

  it("rejects a due time below -1ms", () => {
    const time = new FakeTimeProvider();
    const timer = newTimer(time);

    expect(() => timer.change({ seconds: -5 })).toThrow(/due/i);

    timer.dispose();
  });

  it("rejects a due time above the max supported timeout", () => {
    const time = new FakeTimeProvider();
    const timer = newTimer(time);

    expect(() => timer.change({ ms: MAX_SUPPORTED_TIMEOUT_MS + 1 })).toThrow(/due/i);

    timer.dispose();
  });

  it("rejects a period below -1ms", () => {
    const time = new FakeTimeProvider();
    const timer = newTimer(time);

    expect(() => timer.change({ seconds: 1 }, { seconds: -5 })).toThrow(/period/i);

    timer.dispose();
  });

  it("rejects a period above the max supported timeout", () => {
    const time = new FakeTimeProvider();
    const timer = newTimer(time);

    expect(() => timer.change({ seconds: 1 }, { ms: MAX_SUPPORTED_TIMEOUT_MS + 1 })).toThrow(
      /period/i,
    );

    timer.dispose();
  });

  it("rejects out-of-range due/period passed to the constructor", () => {
    const time = new FakeTimeProvider();

    expect(() => newTimer(time, { seconds: -5 })).toThrow(/due/i);
  });

  it("does not produce an unhandled rejection and keeps ticking when the callback throws", async () => {
    const time = new FakeTimeProvider();
    const unhandled: unknown[] = [];
    const onUnhandledRejection = (reason: unknown) => unhandled.push(reason);
    process.on("unhandledRejection", onUnhandledRejection);
    let ticks = 0;
    const timer = new GrainTimerImpl(
      time,
      (cb) => cb(),
      async () => {
        ticks++;
        throw new Error("boom");
      },
      { ms: 1000 },
      { ms: 1000 },
    );
    try {
      time.advance(1000);
      await flush();
      time.advance(1000);
      await flush();

      expect(ticks).toBe(2);
      expect(unhandled).toHaveLength(0);
    } finally {
      process.off("unhandledRejection", onUnhandledRejection);
      timer.dispose();
    }
  });

  it("reports a callback error to onError instead of throwing it back through fire", async () => {
    const time = new FakeTimeProvider();
    const errors: unknown[] = [];
    const timer = new GrainTimerImpl(
      time,
      (cb) => cb(),
      async () => {
        throw new Error("boom");
      },
      { ms: 1000 },
      undefined,
      (error) => errors.push(error),
    );

    time.advance(1000);
    await flush();

    expect(errors).toHaveLength(1);
    expect((errors[0] as Error).message).toBe("boom");

    timer.dispose();
  });

  it("leaves prior scheduling untouched when change() rejects an invalid value", () => {
    const time = new FakeTimeProvider();
    let fired = false;
    const timer = new GrainTimerImpl(
      time,
      (cb) => cb(),
      async () => {
        fired = true;
      },
      { ms: 1000 },
    );

    expect(() => timer.change({ seconds: -5 })).toThrow();

    time.advance(1000);
    expect(fired).toBe(true);

    timer.dispose();
  });
});
