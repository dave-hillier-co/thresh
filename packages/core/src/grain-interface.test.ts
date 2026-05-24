import { describe, expect, it } from "vitest";
import { defineGrainInterface } from "@tsva/core/grain-interface";

interface ICounter {
  increment(by: number): Promise<number>;
  decrement(by: number): Promise<number>;
  get(): Promise<number>;
}

describe("defineGrainInterface", () => {
  const ICounter = defineGrainInterface<ICounter>("ICounter", {
    methods: ["increment", "decrement", "get"],
    options: { get: { readOnly: true } },
  });

  it("indexes methods by declaration order", () => {
    expect(ICounter.methodId.get("increment")).toBe(0);
    expect(ICounter.methodId.get("decrement")).toBe(1);
    expect(ICounter.methodId.get("get")).toBe(2);
  });

  it("derives a stable interface id from the name", () => {
    const again = defineGrainInterface<ICounter>("ICounter", {
      methods: ["increment", "decrement", "get"],
    });
    expect(ICounter.id).toBe(again.id);
  });

  it("gives different names different ids", () => {
    const other = defineGrainInterface<ICounter>("IOther", { methods: ["increment"] });
    expect(ICounter.id).not.toBe(other.id);
  });

  it("carries per-method invocation options", () => {
    expect(ICounter.options.get?.readOnly).toBe(true);
    expect(ICounter.options.increment).toBeUndefined();
  });
});
