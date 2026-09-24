import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainTimer } from "@thresh/core/grain-timer";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { TimerHandle } from "@thresh/core/time-provider";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface ITicker extends GrainKey<string> {
  startPeriodic(): Promise<void>;
  startOnce(): Promise<void>;
  stop(): Promise<void>;
  getTicks(): Promise<number>;
}
const ITicker = defineGrainInterface<ITicker>("ITicker");

@grain()
class TickerGrain extends Grain implements ITicker {
  private ticks = 0;
  private timer: GrainTimer | undefined;

  async startPeriodic(): Promise<void> {
    this.timer = this.runtime.registerTimer(
      async () => void this.ticks++,
      { seconds: 10 },
      { seconds: 10 },
    );
  }
  async startOnce(): Promise<void> {
    this.timer = this.runtime.registerTimer(async () => void this.ticks++, { seconds: 10 });
  }
  async stop(): Promise<void> {
    this.timer?.dispose();
  }
  async getTicks(): Promise<number> {
    return this.ticks;
  }
}

interface IContextChecker extends GrainKey<string> {
  startWithHeader(): Promise<void>;
  sawHeader(): Promise<string | undefined>;
  startInForeignContext(): Promise<void>;
  sawForeign(): Promise<string | undefined>;
}

/**
 * Stands in for any other `AsyncLocalStorage` a host process runs under —
 * OpenTelemetry's context manager (the active span), a logging scope — which
 * Orleans' `ExecutionContextSuppressor` also keeps out of a timer tick.
 */
const foreignStore = new AsyncLocalStorage<string>();
const IContextChecker = defineGrainInterface<IContextChecker>("IContextChecker");

/**
 * A timer's tick must start with a clean ambient context (Orleans
 * `ExecutionContextSuppressor`), not the registering call's RequestContext —
 * see `startWithHeader` setting a header on the SAME turn that calls
 * `registerTimer`.
 */
@grain()
class ContextCheckerGrain extends Grain implements IContextChecker {
  private header: string | undefined = "not fired yet";
  private timer: GrainTimer | undefined;

  async startWithHeader(): Promise<void> {
    this.runtime.setRequestContext("trace", "caller-value");
    this.timer = this.runtime.registerTimer(
      async () => {
        this.header = this.runtime.getRequestContext("trace");
      },
      { ms: 10 },
    );
  }
  async sawHeader(): Promise<string | undefined> {
    return this.header;
  }

  private foreign: string | undefined = "not fired yet";

  async startInForeignContext(): Promise<void> {
    foreignStore.run("registering-call", () => {
      this.timer = this.runtime.registerTimer(
        async () => {
          this.foreign = foreignStore.getStore();
        },
        { ms: 10 },
      );
    });
  }
  async sawForeign(): Promise<string | undefined> {
    return this.foreign;
  }
}

/**
 * A `FakeTimeProvider` whose fired handler runs back under the
 * `AsyncLocalStorage` context that was active when `setTimer` was called —
 * the same way a REAL `setTimeout`'s callback runs under the context
 * captured at scheduling time (Node propagates `AsyncLocalStorage` through
 * timers automatically). The plain `FakeTimeProvider` fires handlers as a
 * bare function call from whatever context `advance()` happens to run in,
 * which can't reproduce (or verify a fix for) a context leak that only a
 * real timer's context-capturing behaviour causes — this fake exists only
 * for that one test, so it doesn't change the fidelity every other test
 * relies on.
 */
class ContextCapturingTimeProvider extends FakeTimeProvider {
  override setTimer(handler: () => void, delayMs: number): TimerHandle {
    return super.setTimer(AsyncResource.bind(handler), delayMs);
  }
}

const flush = () => new Promise((r) => setTimeout(r, 0));

// Fixed-delay timers (Orleans `GrainTimer.OnTickCompleted`) only arm the next
// period once the previous tick's turn has settled, which happens on a
// microtask `FakeTimeProvider.advance`'s synchronous sweep doesn't drain. So
// observing N periods needs N separate advance-then-flush steps rather than
// one big advance covering all of them.
async function advanceByPeriods(time: FakeTimeProvider, periodMs: number, count: number) {
  for (let i = 0; i < count; i++) {
    time.advance(periodMs);
    await flush();
  }
}

describe("timers", () => {
  let time: FakeTimeProvider;
  let silo: Silo;

  beforeEach(() => {
    time = new FakeTimeProvider();
    silo = new Silo({
      time,
      defaultCollectionAgeSeconds: 100_000,
      collectionIntervalSeconds: 100_000,
    });
    silo.registerGrain(TickerGrain, { interfaces: [ITicker] });
    silo.start();
  });

  it("fires a periodic timer as a turn on each period", async () => {
    const t = silo.getGrain(ITicker, "a");
    await t.startPeriodic();
    await advanceByPeriods(time, 10_000, 3); // three 10s periods
    expect(await t.getTicks()).toBe(3);
  });

  it("fires a one-shot timer exactly once", async () => {
    const t = silo.getGrain(ITicker, "b");
    await t.startOnce();
    time.advance(10_000);
    await flush();
    expect(await t.getTicks()).toBe(1);
    time.advance(30_000);
    await flush();
    expect(await t.getTicks()).toBe(1);
  });

  it("stops firing once disposed", async () => {
    const t = silo.getGrain(ITicker, "c");
    await t.startPeriodic();
    await advanceByPeriods(time, 10_000, 2);
    expect(await t.getTicks()).toBe(2);
    await t.stop();
    time.advance(50_000);
    await flush();
    expect(await t.getTicks()).toBe(2);
  });
});

describe("timer ambient context", () => {
  it("starts each tick with a clean ambient context, not the registering call's RequestContext", async () => {
    const ctxTime = new ContextCapturingTimeProvider();
    const ctxSilo = new Silo({
      time: ctxTime,
      defaultCollectionAgeSeconds: 100_000,
      collectionIntervalSeconds: 100_000,
    });
    ctxSilo.registerGrain(ContextCheckerGrain, { interfaces: [IContextChecker] });
    ctxSilo.start();

    const c = ctxSilo.getGrain(IContextChecker, "ctx");
    await c.startWithHeader();
    ctxTime.advance(10);
    await flush();
    expect(await c.sawHeader()).toBeUndefined();
  });

  it("does not carry any other async-local context (e.g. an active trace span) into a tick", async () => {
    const ctxTime = new ContextCapturingTimeProvider();
    const ctxSilo = new Silo({
      time: ctxTime,
      defaultCollectionAgeSeconds: 100_000,
      collectionIntervalSeconds: 100_000,
    });
    ctxSilo.registerGrain(ContextCheckerGrain, { interfaces: [IContextChecker] });
    ctxSilo.start();

    const c = ctxSilo.getGrain(IContextChecker, "foreign");
    await c.startInForeignContext();
    ctxTime.advance(10);
    await flush();
    expect(await c.sawForeign()).toBeUndefined();
  });
});
