import { describe, expect, it } from "vitest";
import { defineGrainInterface } from "@thresh/core/grain-interface";

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

  it("defaults to version 1 and carries an explicit version", () => {
    expect(defineGrainInterface("IVersioned.default").version).toBe(1);
    expect(defineGrainInterface("IVersioned.explicit", { version: 3 }).version).toBe(3);
  });

  it("keeps the id stable across versions of one interface (id is name-derived)", () => {
    const v1 = defineGrainInterface("IVersioned.same", { version: 1 });
    const v2 = defineGrainInterface("IVersioned.same", { version: 2 });
    expect(v1.id).toBe(v2.id);
    expect(v1.version).not.toBe(v2.version);
  });
});
