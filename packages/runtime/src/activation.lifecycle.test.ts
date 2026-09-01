import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { Grain } from "@thresh/core/grain";
import type { GrainKey } from "@thresh/core/key-kinds";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface ICounter extends GrainKey<string> {
  increment(by: number): Promise<number>;
  get(): Promise<number>;
}

const ICounter = defineGrainInterface<ICounter>("ICounter.lifecycle", {
  options: { get: { readOnly: true } },
});

interface IBadActivate extends GrainKey<string> {
  ping(): Promise<string>;
}

const IBadActivate = defineGrainInterface<IBadActivate>("IBadActivate.lifecycle");

@grain()
class BadActivateGrain extends Grain implements IBadActivate {
  override async onActivate(): Promise<void> {
    throw new Error("boom");
  }

  async ping(): Promise<string> {
    return "pong";
  }
}

// Module-scoped sink the in-process grain writes lifecycle events to.
let events: string[] = [];

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;

  override async onActivate(): Promise<void> {
    events.push(`activate:${String(this.id.key)}`);
  }

  override async onDeactivate(): Promise<void> {
    events.push(`deactivate:${String(this.id.key)}`);
  }

  async increment(by: number): Promise<number> {
    events.push(`increment:${by}`);
    this.count += by;
    return this.count;
  }

  async get(): Promise<number> {
    return this.count;
  }
}

const flush = () => new Promise((r) => setTimeout(r, 0));
const counterId = (key: string) => new GrainId("Counter", key);

describe("activation lifecycle (sociable)", () => {
  let time: FakeTimeProvider;
  let silo: Silo;

  beforeEach(() => {
    events = [];
    time = new FakeTimeProvider();
    silo = new Silo({ time, defaultCollectionAgeSeconds: 30, collectionIntervalSeconds: 10 });
    silo.registerGrain(CounterGrain, { interfaces: [ICounter] });
    silo.start();
  });

  it("activates on first call and runs onActivate before the first message", async () => {
    const counter = silo.getGrain(ICounter, "a");
    const result = await counter.increment(5);
    expect(result).toBe(5);
    expect(events).toEqual(["activate:a", "increment:5"]);
    expect(silo.isActive(counterId("a"))).toBe(true);
  });

  it("serializes concurrent calls into single-threaded turns", async () => {
    const counter = silo.getGrain(ICounter, "a");
    await Promise.all([counter.increment(1), counter.increment(2), counter.increment(3)]);
    expect(await counter.get()).toBe(6);
  });

  it("deactivates when idle and reactivates fresh on the next call", async () => {
    const counter = silo.getGrain(ICounter, "a");
    await counter.increment(5);
    expect(silo.isActive(counterId("a"))).toBe(true);

    // Move past the 30s collection age; the 10s collector sweep deactivates it.
    time.advance(31_000);
    await flush();

    expect(events).toContain("deactivate:a");
    expect(silo.isActive(counterId("a"))).toBe(false);

    // Next call reactivates with fresh state.
    const result = await counter.increment(1);
    expect(result).toBe(1);
    expect(events.filter((e) => e === "activate:a")).toHaveLength(2);
  });

  it("keeps distinct keys as independent activations", async () => {
    const a = silo.getGrain(ICounter, "a");
    const b = silo.getGrain(ICounter, "b");
    await a.increment(10);
    await b.increment(1);
    expect(await a.get()).toBe(10);
    expect(await b.get()).toBe(1);
  });
});

describe("failed activation surfaces the original error", () => {
  let silo: Silo;

  beforeEach(() => {
    silo = new Silo({
      time: new FakeTimeProvider(),
      defaultCollectionAgeSeconds: 30,
      collectionIntervalSeconds: 10,
    });
    silo.registerGrain(BadActivateGrain, { interfaces: [IBadActivate] });
    silo.start();
  });

  it("rejects a call to an onActivate that throws with the original message", async () => {
    const grain = silo.getGrain(IBadActivate, "x");
    await expect(grain.ping()).rejects.toThrow("boom");
    // Not the generic fallback.
    await expect(grain.ping()).rejects.not.toThrow("activation unavailable");
  });

  it("settles every one of many concurrent calls to a failing activation", async () => {
    const grain = silo.getGrain(IBadActivate, "y");
    const results = await Promise.allSettled(Array.from({ length: 50 }, () => grain.ping()));
    expect(results).toHaveLength(50);
    for (const r of results) {
      expect(r.status).toBe("rejected");
      expect((r as PromiseRejectedResult).reason).toHaveProperty("message", "boom");
    }
  });
});
