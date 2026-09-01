import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainTimer } from "@thresh/core/grain-timer";
import type { GrainKey } from "@thresh/core/key-kinds";
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

const flush = () => new Promise((r) => setTimeout(r, 0));

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
    time.advance(30_000); // three 10s periods
    await flush();
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
    time.advance(20_000);
    await flush();
    expect(await t.getTicks()).toBe(2);
    await t.stop();
    time.advance(50_000);
    await flush();
    expect(await t.getTicks()).toBe(2);
  });
});
