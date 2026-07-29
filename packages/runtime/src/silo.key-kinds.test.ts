import { beforeEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { Silo } from "@thresh/runtime/silo";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// A marker-free interface that declares its key kind on the definition. Under
// the legacy marker path this would resolve to a string key; the declared kind
// is what makes `getGrain(ICounter, 1n)` the correct call.
interface ICounter {
  increment(by: number): Promise<number>;
}

const ICounter = defineGrainInterface<ICounter, "integer">("ICounter.silo-key-kinds", {
  key: "integer",
});

const CounterGrain = defineGrain<ICounter>("KeyKindCounter", () => {
  let count = 0;
  return { increment: async (by: number) => (count += by) };
});

interface ICaller {
  bump(target: bigint): Promise<number>;
}

const ICaller = defineGrainInterface<ICaller>("ICaller.silo-key-kinds");

const CallerGrain = defineGrain<ICaller>("KeyKindCaller", (ctx) => ({
  bump: (target: bigint) => ctx.getGrain(ICounter, target).increment(1),
}));

describe("declared key kinds through the silo (sociable)", () => {
  let silo: Silo;

  beforeEach(() => {
    silo = new Silo({ time: new FakeTimeProvider() });
    silo.registerGrain(CounterGrain.grain, { interfaces: [ICounter] });
    silo.registerGrain(CallerGrain.grain, { interfaces: [ICaller] });
    silo.start();
  });

  it("addresses a declared integer-keyed grain with a bigint from the silo", async () => {
    const counter = silo.getGrain(ICounter, 7n);

    expect(await counter.increment(5)).toBe(5);
    expect(await silo.getGrain(ICounter, 7n).increment(2)).toBe(7);
    expect(await silo.getGrain(ICounter, 8n).increment(1)).toBe(1);
  });

  it("addresses it with a bigint from inside another grain", async () => {
    await silo.getGrain(ICounter, 7n).increment(4);

    expect(await silo.getGrain(ICaller, "a").bump(7n)).toBe(5);
  });
});
