import { describe, expect, it } from "vitest";
import { defineGrainInterface } from "@tsva/core/grain-interface";

interface ICounter {
  increment(by: number): Promise<number>;
  decrement(by: number): Promise<number>;
  get(): Promise<number>;
}

describe("defineGrainInterface", () => {
  const ICounter = defineGrainInterface<ICounter>("ICounter", {
    options: { get: { readOnly: true } },
  });

  it("derives a stable interface id from the name", () => {
    const again = defineGrainInterface<ICounter>("ICounter");
    expect(ICounter.id).toBe(again.id);
  });

  it("gives different names different ids", () => {
    const other = defineGrainInterface<ICounter>("IOther");
    expect(ICounter.id).not.toBe(other.id);
  });

  it("carries per-method invocation options (no method table to declare)", () => {
    expect(ICounter.options.get?.readOnly).toBe(true);
    expect(ICounter.options.increment).toBeUndefined();
  });
});
