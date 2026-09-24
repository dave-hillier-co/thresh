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

  it("arms the next tick only once the previous callback settles (fixed-delay), never overlapping or queuing", async () => {
    // Orleans arms the next tick only after the callback completes
    // (`GrainTimer.OnTickCompleted`), so ticks of one timer never overlap or
    // queue even when a callback outlasts the period.
    const time = new FakeTimeProvider();
    let armCount = 0;
    const originalSetTimer = time.setTimer.bind(time);
    time.setTimer = (handler, delayMs) => {
      armCount++;
      return originalSetTimer(handler, delayMs);
    };
    let resolveCallback: (() => void) | undefined;
    const callback = () =>
      new Promise<void>((resolve) => {
        resolveCallback = resolve;
      });
    const timer = new GrainTimerImpl(time, (cb) => cb(), callback, { ms: 5 }, { ms: 5 });

    expect(armCount).toBe(1); // the initial due-time schedule from the constructor

    time.advance(5);
    await flush();
    // The callback is still pending — the next tick must not have been armed yet.
    expect(armCount).toBe(1);

    resolveCallback?.();
    await flush();
    // Only now that the callback settled is the next period armed.
    expect(armCount).toBe(2);

    timer.dispose();
  });

  it("defers a change() made during a tick until that tick settles, leaving one armed timer", async () => {
    // Orleans' `GrainTimer.Change` only records the new due/period while the
    // timer is firing (`_firing`), and `OnTickCompleted` then arms exactly one
    // next tick from the changed due time. Arming immediately AND again when
    // the tick settles would leave two independent tick chains running.
    const time = new FakeTimeProvider();
    let ticks = 0;
    let timer: GrainTimerImpl | undefined;
    let resolveFirst: (() => void) | undefined;
    const callback = () => {
      ticks++;
      if (ticks === 1) {
        timer!.change({ ms: 100 }, { ms: 100 });
        return new Promise<void>((resolve) => {
          resolveFirst = resolve;
        });
      }
      return Promise.resolve();
    };
    timer = new GrainTimerImpl(time, (cb) => cb(), callback, { ms: 10 }, { ms: 10 });

    time.advance(10);
    await flush();
    expect(ticks).toBe(1);

    // Still inside the first tick: the change must not have fired a tick yet.
    time.advance(100);
    await flush();
    expect(ticks).toBe(1);

    resolveFirst?.();
    await flush();

    for (let i = 0; i < 5; i++) {
      time.advance(100);
      await flush();
    }
    // One tick per 100ms period after the first tick settled — not two chains.
    expect(ticks).toBe(6);

    timer.dispose();
  });

  it("never fires for an infinite (-1ms) due time", async () => {
    const time = new FakeTimeProvider();
    let ticks = 0;
    const timer = new GrainTimerImpl(
      time,
      (cb) => cb(),
      async () => {
        ticks++;
      },
      { ms: -1 },
      { ms: 10 },
    );

    time.advance(1000);
    await flush();
    expect(ticks).toBe(0);

    timer.dispose();
  });

  it("fires once and stops for an infinite (-1ms) period", async () => {
    // Orleans treats `Timeout.InfiniteTimeSpan` as "disabled": a -1ms period
    // is a one-shot timer, not a re-arm at (clamped) zero delay.
    const time = new FakeTimeProvider();
    let ticks = 0;
    const timer = new GrainTimerImpl(
      time,
      (cb) => cb(),
      async () => {
        ticks++;
      },
      { ms: 10 },
      { ms: -1 },
    );

    for (let i = 0; i < 5; i++) {
      time.advance(10);
      await flush();
    }
    expect(ticks).toBe(1);

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
