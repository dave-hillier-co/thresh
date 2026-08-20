import { beforeEach, describe, expect, it } from "vitest";
import { defineGrain, setupDepth } from "@thresh/core/define-grain";
import { INCOMING_CALL_FILTER } from "@thresh/core/grain-call-filter";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

interface IFnCounter extends GrainWithStringKey {
  increment(by: number): Promise<number>;
  get(): Promise<number>;
}

const IFnCounter = defineGrainInterface<IFnCounter>("IFnCounter.define-grain-activation");

// Module-scoped sink the functional grain writes lifecycle events to.
let events: string[] = [];

const FnCounterGrain = defineGrain<IFnCounter>("FnCounter", (ctx) => {
  events.push(`factory:${String(ctx.id.key)}`);
  let count = 0;
  return {
    [INCOMING_CALL_FILTER]: async (call) => {
      events.push(`filter:${call.methodName}`);
      await call.invoke();
    },
    onActivate: () => {
      events.push(`activate:${String(ctx.id.key)}`);
    },
    onDeactivate: () => {
      events.push(`deactivate:${String(ctx.id.key)}`);
    },
    increment: async (by: number) => (count += by),
    get: async () => count,
  };
});

const flush = (): Promise<unknown> => new Promise((r) => setTimeout(r, 0));

describe("defineGrain through the catalog (sociable)", () => {
  let time: FakeTimeProvider;
  let silo: Silo;

  beforeEach(() => {
    events = [];
    time = new FakeTimeProvider();
    silo = new Silo({ time, defaultCollectionAgeSeconds: 30, collectionIntervalSeconds: 10 });
    silo.registerGrain(FnCounterGrain.grain, { interfaces: [IFnCounter] });
    silo.start();
  });

  it("runs the factory once per activation, before onActivate, and dispatches to its members", async () => {
    const counter = silo.getGrain(IFnCounter, "a");

    expect(await counter.increment(5)).toBe(5);
    expect(await counter.increment(2)).toBe(7);

    expect(events.filter((e) => e === "factory:a")).toHaveLength(1);
    expect(events.slice(0, 2)).toEqual(["factory:a", "activate:a"]);
  });

  it("reaches the symbol-keyed self incoming-call filter on every call", async () => {
    const counter = silo.getGrain(IFnCounter, "a");
    await counter.increment(1);
    await counter.get();

    expect(events.filter((e) => e.startsWith("filter:"))).toEqual([
      "filter:increment",
      "filter:get",
    ]);
  });

  it("reaches onDeactivate on collection and re-runs the factory on reactivation", async () => {
    const counter = silo.getGrain(IFnCounter, "a");
    await counter.increment(5);

    time.advance(31_000);
    await flush();

    expect(events).toContain("deactivate:a");
    expect(silo.isActive(new GrainId("FnCounter", "a"))).toBe(false);

    // A fresh activation runs the factory again, so closure state starts over.
    expect(await counter.increment(1)).toBe(1);
    expect(events.filter((e) => e === "factory:a")).toHaveLength(2);
  });

  it("leaves no setup on the stack once an activation is created", async () => {
    const counter = silo.getGrain(IFnCounter, "a");
    await counter.increment(1);

    // `Catalog.create` asserts this on entry: the window between `createInstance`
    // and `setContext` is load-bearing and must stay synchronous.
    expect(setupDepth()).toBe(0);
  });

  it("keeps distinct keys on independent closure state", async () => {
    const a = silo.getGrain(IFnCounter, "a");
    const b = silo.getGrain(IFnCounter, "b");
    await a.increment(10);
    await b.increment(1);

    expect(await a.get()).toBe(10);
    expect(await b.get()).toBe(1);
  });
});
