// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/TimerOrleansTest.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { durationToMs, type Duration } from "@thresh/core/duration";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import {
  NonReentrantTimerCallGrain,
  INonReentrantTimerCallGrain,
} from "@thresh/parity/grains/impl/non-reentrant-timer-call-grain";
import { ITimerCallGrain, TimerCallGrain } from "@thresh/parity/grains/impl/timer-call-grain";
import { ITimerGrain, TimerGrain } from "@thresh/parity/grains/impl/timer-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

// Lets pending timer-callback turns (scheduled via the fake clock) run to
// completion before the test observes grain state.
const flush = () => new Promise((resolve) => setTimeout(resolve, 0));

// `FakeTimeProvider.advance` fires every due timer in one synchronous sweep, without
// draining microtasks in between — so a busy periodic timer can make the turn
// scheduler look continuously "busy" for the whole span, starving an idle-collection
// sweep landing in the same window. Stepping the advance and flushing between steps
// gives the scheduler a chance to drain so idle collection is fairly considered.
async function advanceStepped(
  time: FakeTimeProvider,
  totalMs: number,
  stepMs = 100,
): Promise<void> {
  let remaining = totalMs;
  while (remaining > 0) {
    const step = Math.min(stepMs, remaining);
    time.advance(step);
    await flush();
    remaining -= step;
  }
}

describe("DefaultCluster.Tests.TimerTests.TimerOrleansTest", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 2,
      time,
      grains: [
        { ctor: TimerGrain, interfaces: [ITimerGrain] },
        { ctor: TimerCallGrain, interfaces: [ITimerCallGrain] },
        { ctor: NonReentrantTimerCallGrain, interfaces: [INonReentrantTimerCallGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.TimerOrleansTest_Basic",
    async () => {
      for (let i = 0; i < 10; i++) {
        const grain = cluster.getGrain(ITimerGrain, randomIntegerKey());
        const periodMs = durationToMs(await grain.getTimerPeriod());
        time.advance(periodMs * 10);
        await flush();
        const last = await grain.getCounter();
        expect(last >= 10 && last <= 12, String(last)).toBe(true);

        await grain.stopDefaultTimer();
        time.advance(periodMs * 2);
        await flush();
        const curr = await grain.getCounter();
        expect(curr === last || curr === last + 1, "curr == last || curr == last + 1").toBe(true);
      }
    },
  );

  orleansTest(
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.TimerOrleansTest_Parallel",
    async () => {
      const grains: ITimerGrain[] = [];
      let periodMs = 0;
      for (let i = 0; i < 10; i++) {
        const grain = cluster.getGrain(ITimerGrain, randomIntegerKey());
        grains.push(grain);
        periodMs = durationToMs(await grain.getTimerPeriod()); // activate the grain
      }

      time.advance(periodMs * 10);
      await flush();

      for (const grain of grains) {
        const last = await grain.getCounter();
        expect(last >= 10 && last <= 12, "last >= 10 && last <= 12").toBe(true);
      }

      for (const grain of grains) {
        await grain.stopDefaultTimer();
      }
    },
  );

  orleansTest(
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.AsyncTimerTest_GrainCall",
    async () => {
      const grain = cluster.getGrain(ITimerCallGrain, randomIntegerKey());
      await grain.startTimer("async-timer-call", { seconds: 1 });
      time.advance(1000);
      await flush();
      expect(await grain.getTickCount()).toBe(1);
      // A grain-method call's "no value" result is `null` regardless of
      // whether the call landed same-silo or cross-silo (GAP-BUG-LOCAL-CALL-UNDEFINED,
      // now fixed): `GrainFactory`'s caller-facing return path normalizes a
      // local `undefined` to `null`, matching what the message serializer
      // already produces for a cross-silo call.
      expect(await grain.getException()).toBeNull();
      await grain.stopTimer("async-timer-call");
    },
  );

  orleansTest.excluded(
    ".NET-specific: exercises IGrainTimer registration API overloads (state-object vs " +
      "closure-based delegate signatures) which don't exist as distinct overloads in " +
      "this framework's single registerTimer(callback, due, period?) signature",
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.GrainTimer_TestAllOverloads",
  );

  // Upstream registers the self-disposing timer with `Interleave = true`
  // (`GrainTimerCreationOptions.Interleave`) specifically so the timer's own callback
  // turn can run concurrently with the outer call awaiting it on a non-reentrant
  // grain — otherwise the outer call and the timer callback deadlock (the turn
  // scheduler never admits a second exclusive turn while one is still running, and
  // the outer call never settles until the timer's callback runs). This framework's
  // `registerTimer` has no per-timer interleave option (`GrainRuntime.registerTimer`
  // hard-codes `{}` invoke options for every timer turn), so the same pattern
  // deadlocks here instead.
  orleansTest(
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.GrainTimer_DisposeFromCallback",
    async () => {
      // The grain registers an interleaving self-disposing timer and awaits its
      // callback; on a non-reentrant grain the callback can only run (and let
      // the outer call settle) because it interleaves. Start the call, fire the
      // due timer, then confirm the outer call completes rather than deadlocks.
      const grain = cluster.getGrain(ITimerCallGrain, randomIntegerKey());
      const running = grain.runSelfDisposingTimer();
      await flush();
      time.advance(1);
      await flush();
      await running;
    },
  );

  orleansTest(
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.NonReentrantGrainTimer_Test",
    async () => {
      const grain = cluster.getGrain(INonReentrantTimerCallGrain, randomIntegerKey());
      await grain.startTimer("non-reentrant-timer", { seconds: 1 });
      time.advance(1000);
      await flush();
      expect(await grain.getTickCount()).toBe(1);
      // See AsyncTimerTest_GrainCall above: null regardless of placement now
      // that GAP-BUG-LOCAL-CALL-UNDEFINED is fixed.
      expect(await grain.getException()).toBeNull();
      await grain.stopTimer("non-reentrant-timer");
    },
  );

  // Ports the due-time/period range-validation portion of upstream's
  // GrainTimer_Change (invalid values now throw, matching Orleans'
  // TimerQueueTimer.ValidateArguments — see packages/runtime/src/grain-timer-impl.ts),
  // plus upstream's `grain2`/`grain3` sections: the timer callback itself calling
  // Change()/dispose() via the `operationType` argument (see
  // packages/parity/src/grains/impl/timer-call-grain.ts).
  orleansTest("DefaultCluster.Tests.TimerTests.TimerOrleansTest.GrainTimer_Change", async () => {
    const testName = "GrainTimer_Change";
    const delay: Duration = { seconds: 5 };
    const grain = cluster.getGrain(ITimerCallGrain, randomIntegerKey());

    await grain.startTimer(testName, delay);
    time.advance(durationToMs(delay) * 2);
    await flush();
    expect(await grain.getTickCount()).toBe(1);

    await grain.restartTimer(testName, delay);
    time.advance(durationToMs(delay) * 2);
    await flush();
    expect(await grain.getTickCount()).toBe(2);

    // Zero and sub-ms timeouts should be valid (truncated toward zero, as the
    // `(long)` cast in Orleans' ValidateArguments does).
    await grain.restartTimer(testName, { ms: 0 });
    await grain.restartTimer(testName, { ms: 0.01 });
    await grain.restartTimer(testName, { ms: 0 }, { ms: 0 });
    await grain.restartTimer(testName, { ms: 0.01 }, { ms: 0.01 });
    await grain.restartTimer(testName, { ms: -0.4 });
    await grain.restartTimer(testName, { seconds: 1 }, { ms: -0.5 });

    // Invalid values.
    await expect(grain.restartTimer(testName, { seconds: -5 })).rejects.toThrow();
    await expect(grain.restartTimer(testName, { days: 100_000 })).rejects.toThrow();
    await expect(grain.restartTimer(testName, { seconds: 1 }, { seconds: -5 })).rejects.toThrow();
    await expect(grain.restartTimer(testName, { seconds: 1 }, { days: 100_000 })).rejects.toThrow();

    // `null` regardless of placement (GAP-BUG-LOCAL-CALL-UNDEFINED, now fixed
    // — see AsyncTimerTest_GrainCall above).
    expect(await grain.getException()).toBeNull();

    await grain.stopTimer(testName);

    // grain2: the timer's own callback calls `timer.change()` (operationType
    // "update_period"), pushing the next fire far out — the tick count should
    // stay at 1 for the remainder of `wait`.
    const grain2 = cluster.getGrain(ITimerCallGrain, randomIntegerKey());
    await grain2.startTimer(testName, delay, "update_period");
    time.advance(durationToMs(delay) * 2);
    await flush();
    expect(await grain2.getException()).toBeNull();
    expect(await grain2.getTickCount()).toBe(1);

    // grain3: the timer's own callback disposes it (operationType
    // "dispose_timer") — it should tick exactly once and never again.
    const grain3 = cluster.getGrain(ITimerCallGrain, randomIntegerKey());
    await grain3.startTimer(testName, delay, "dispose_timer");
    time.advance(durationToMs(delay) * 4);
    await flush();
    expect(await grain3.getException()).toBeNull();
    expect(await grain3.getTickCount()).toBe(1);
  });

  orleansTest.excluded(
    "POCO-grain variant: this framework has one grain model (no separate POCO-vs-" +
      "Grain-derived activation style as in Orleans), so it doesn't test anything the " +
      "non-POCO variant doesn't already cover",
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.TimerOrleansTest_Basic_Poco",
  );

  orleansTest.excluded(
    "POCO-grain variant: this framework has one grain model (no separate POCO-vs-" +
      "Grain-derived activation style as in Orleans), so it doesn't test anything the " +
      "non-POCO variant doesn't already cover",
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.TimerOrleansTest_Parallel_Poco",
  );

  orleansTest.excluded(
    "POCO-grain variant: this framework has one grain model (no separate POCO-vs-" +
      "Grain-derived activation style as in Orleans), so it doesn't test anything the " +
      "non-POCO variant doesn't already cover",
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.TimerOrleansTest_Migration_Poco",
  );

  orleansTest.excluded(
    "POCO-grain variant: this framework has one grain model (no separate POCO-vs-" +
      "Grain-derived activation style as in Orleans), so it doesn't test anything the " +
      "non-POCO variant doesn't already cover",
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.AsyncTimerTest_GrainCall_Poco",
  );

  orleansTest.excluded(
    ".NET-specific overload test (see GrainTimer_TestAllOverloads), also a POCO-grain " +
      "variant of it",
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.GrainTimer_TestAllOverloads_Poco",
  );

  orleansTest.excluded(
    "POCO-grain variant: this framework has one grain model (no separate POCO-vs-" +
      "Grain-derived activation style as in Orleans), so it doesn't test anything the " +
      "non-POCO variant doesn't already cover",
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.NonReentrantGrainTimer_Test_Poco",
  );

  orleansTest.excluded(
    "POCO-grain variant of GrainTimer_Change, also GAP-TIMER-VALIDATION",
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.GrainTimer_Change_Poco",
  );
});

// A dedicated cluster/clock: this test relies on the idle-collection sweep landing
// within a single 60s `advance`, which only holds if the fake clock starts fresh at 0
// (the collector's next sweep is a fixed 60s after cluster start).
describe("DefaultCluster.Tests.TimerTests.TimerOrleansTest (migration)", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 2,
      time,
      grains: [{ ctor: TimerGrain, interfaces: [ITimerGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.TimerTests.TimerOrleansTest.TimerOrleansTest_Migration",
    async () => {
      const grain = cluster.getGrain(ITimerGrain, randomIntegerKey());
      const periodMs = durationToMs(await grain.getTimerPeriod());

      // Ensure that the grain works as it should.
      time.advance(periodMs * 10);
      await flush();
      await grain.getCounter();

      // Restart the grain: `deactivate()` requests deactivation on idle, which the
      // fake clock's advance past the idle-collection sweep interval carries out.
      await grain.deactivate();
      await advanceStepped(time, 60_000);
      let last = await grain.getCounter();
      expect(last === 0, "Restarted grains should have zero ticks. Actual: " + last).toBe(true);

      // Poke the grain and ensure it still works as it should.
      const elapsedMs = periodMs * 10;
      time.advance(elapsedMs);
      await flush();
      last = await grain.getCounter();

      const maximalNumTicks = Math.ceil(elapsedMs / periodMs);
      expect(
        last <= maximalNumTicks,
        `Assert: last <= maximalNumTicks. Actual: last = ${last}, maximalNumTicks = ${maximalNumTicks}`,
      ).toBe(true);
    },
  );
});
