import { describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { defineReducerGrain, type ReducerResult } from "@thresh/core/define-reducer-grain";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { normalizeRegistration } from "@thresh/core/grain-registration";

interface IGreeter {
  greet(name: string): Promise<string>;
}

const Greeter = defineGrain("RegistrationGreeter", () => ({
  greet: async (name: string) => `hello ${name}`,
}));

const ISeparateGreeter = defineGrainInterface<IGreeter>("registration.ISeparateGreeter");
const IExtraGreeter = defineGrainInterface<IGreeter>("registration.IExtraGreeter");

class PlainGrain extends Grain {}

interface Counter {
  count: number;
}

const Counting = defineReducerGrain<Counter, { type: "add" }>("RegistrationCounting", {
  initial: () => ({ count: 0 }),
  reduce: (state): ReducerResult<Counter> => ({ state: { count: state.count + 1 } }),
});

describe("normalizeRegistration", () => {
  it("registers a bare definition under its own interface", () => {
    const { ctor, interfaces } = normalizeRegistration(Greeter);
    expect(ctor).toBe(Greeter.grain);
    expect(interfaces).toEqual([Greeter]);
  });

  it("registers a reducer grain under its own interface", () => {
    const { ctor, interfaces } = normalizeRegistration(Counting);
    expect(ctor).toBe(Counting.grain);
    expect(interfaces).toEqual([Counting]);
  });

  it("registers a plain constructor under the interfaces it is given", () => {
    const { ctor, interfaces } = normalizeRegistration(PlainGrain, {
      interfaces: [ISeparateGreeter],
    });
    expect(ctor).toBe(PlainGrain);
    expect(interfaces).toEqual([ISeparateGreeter]);
  });

  it("registers a constructor under several interfaces", () => {
    const { interfaces } = normalizeRegistration(PlainGrain, {
      interfaces: [ISeparateGreeter, IExtraGreeter],
    });
    expect(interfaces).toEqual([ISeparateGreeter, IExtraGreeter]);
  });

  // The migrated call sites read `registerGrain(X.grain, { interfaces: [ISeparate] })`,
  // where `ISeparate` is declared apart from the fused definition. Appending the
  // definition's own interface would register those grains under two interfaces
  // where they register one today — a behaviour change, not a rewrite.
  it("uses exactly the interfaces given, without appending the definition's own", () => {
    const { ctor, interfaces } = normalizeRegistration(Greeter, {
      interfaces: [ISeparateGreeter],
    });
    expect(ctor).toBe(Greeter.grain);
    expect(interfaces).toEqual([ISeparateGreeter]);
  });

  it("does not alias the caller's array", () => {
    const list = [ISeparateGreeter];
    const { interfaces } = normalizeRegistration(PlainGrain, { interfaces: list });
    expect(interfaces).not.toBe(list);
    interfaces.push(IExtraGreeter);
    expect(list).toHaveLength(1);
  });

  it("rejects a bare constructor with no interfaces", () => {
    expect(() => normalizeRegistration(PlainGrain)).toThrow(/PlainGrain.*\{ interfaces/s);
  });

  it("rejects an empty interfaces list", () => {
    expect(() => normalizeRegistration(PlainGrain, { interfaces: [] })).toThrow(
      /at least one interface/,
    );
    expect(() => normalizeRegistration(Greeter, { interfaces: [] })).toThrow(
      /at least one interface/,
    );
  });
});
