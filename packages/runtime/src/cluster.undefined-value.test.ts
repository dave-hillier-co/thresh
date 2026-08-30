import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

/**
 * A grain method whose return type is `T | undefined` and which returns `undefined` — the shape
 * every ported .NET `Task<T?>` takes. Serialization must not turn that into `null`: a caller
 * written against the declared type tests `result === undefined`, and a `null` slips past that
 * guard to be used as a value. The failure only appears CROSS-SILO, because a same-silo call hands
 * the value back by reference and never serializes it.
 */
interface IMaybeGrain extends GrainWithStringKey {
  /** Returns `undefined` always. */
  missing(): Promise<Uint8Array | undefined>;
  /** Returns a real value, so the absent case is distinguishable from a broken method. */
  present(): Promise<Uint8Array | undefined>;
  /** A `void` method: its `undefined` result must survive the same path. */
  nothing(): Promise<void>;
  /** `undefined` nested in a returned record must stay `undefined` too. */
  record(): Promise<{ readonly value: string | undefined }>;
  /** Reports what the callee actually received in an explicitly-`undefined` ARGUMENT slot. */
  describeArg(first: number, second?: string | undefined): Promise<string>;
  /** The same question for an `undefined` element of an array argument. */
  describeElement(items: readonly (string | undefined)[]): Promise<string>;
  /** And for an `undefined` VALUE in a Map argument, whose key must stay present. */
  describeMapValue(byName: ReadonlyMap<string, string | undefined>): Promise<string>;
}

const IMaybeGrain = defineGrainInterface<IMaybeGrain>("test.IMaybeGrain.undefinedReturn");

@grain()
class MaybeGrain extends Grain implements IMaybeGrain {
  async missing(): Promise<Uint8Array | undefined> {
    return undefined;
  }
  async present(): Promise<Uint8Array | undefined> {
    return new Uint8Array([1, 2, 3]);
  }
  async nothing(): Promise<void> {
    return undefined;
  }
  async record(): Promise<{ readonly value: string | undefined }> {
    return { value: undefined };
  }
  async describeArg(_first: number, second?: string | undefined): Promise<string> {
    return second === undefined ? "undefined" : second === null ? "null" : typeof second;
  }
  async describeElement(items: readonly (string | undefined)[]): Promise<string> {
    const item = items[1];
    return item === undefined ? "undefined" : item === null ? "null" : typeof item;
  }
  async describeMapValue(byName: ReadonlyMap<string, string | undefined>): Promise<string> {
    if (!byName.has("a")) return "absent";
    const value = byName.get("a");
    return value === undefined ? "undefined" : value === null ? "null" : typeof value;
  }
}

/** Two silos, `random -> 0` so every activation lands on silo-0 and silo-1's calls cross the wire. */
function buildCluster() {
  return TestCluster.start({
    clusterId: "undefined-return-cluster",
    initialSilos: 2,
    random: () => 0,
    grains: [{ ctor: MaybeGrain, interfaces: [IMaybeGrain] }],
  });
}

describe("a grain method returning undefined", () => {
  it("arrives at a CROSS-SILO caller as undefined, not null", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const result = await caller.host.getGrain(IMaybeGrain, "m1").missing();
      expect(result).toBeUndefined();
      expect(result).not.toBeNull();
    } finally {
      await cluster.dispose();
    }
  });

  it("still carries a PRESENT value across the same path", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const result = await caller.host.getGrain(IMaybeGrain, "m2").present();
      expect(result).toEqual(new Uint8Array([1, 2, 3]));
    } finally {
      await cluster.dispose();
    }
  });

  it("returns undefined for a void method cross-silo", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const result = await caller.host.getGrain(IMaybeGrain, "m3").nothing();
      expect(result).toBeUndefined();
    } finally {
      await cluster.dispose();
    }
  });

  it("keeps an undefined MEMBER of a returned record undefined", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const result = await caller.host.getGrain(IMaybeGrain, "m4").record();
      expect(result.value).toBeUndefined();
    } finally {
      await cluster.dispose();
    }
  });

  it("delivers undefined to a SAME-SILO caller too, so the shape does not depend on placement", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[0]!;
      expect(await caller.host.getGrain(IMaybeGrain, "m5").missing()).toBeUndefined();
    } finally {
      await cluster.dispose();
    }
  });
});

/**
 * The mirror image, and the one that actually breaks a ported signature: an OPTIONAL parameter
 * passed explicitly as `undefined`. Arguments travel as a positional ARRAY, where there is no
 * "omit the key" escape an object has - so without a representation for `undefined` the slot
 * arrives as `null` and the callee's `signal === undefined` (or `x ?? default`) guard is wrong.
 */
describe("an undefined ARGUMENT crossing a grain call", () => {
  it("arrives at a CROSS-SILO callee as undefined, not null", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      expect(await caller.host.getGrain(IMaybeGrain, "a1").describeArg(1, undefined)).toBe(
        "undefined",
      );
    } finally {
      await cluster.dispose();
    }
  });

  it("keeps an undefined ELEMENT of an array argument undefined", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      expect(
        await caller.host.getGrain(IMaybeGrain, "a2").describeElement(["x", undefined, "z"]),
      ).toBe("undefined");
    } finally {
      await cluster.dispose();
    }
  });

  it("keeps an undefined Map VALUE undefined, with its key still present", async () => {
    const cluster = await buildCluster();
    try {
      const caller: TestSiloHandle = cluster.silos[1]!;
      const byName = new Map<string, string | undefined>([["a", undefined]]);
      expect(await caller.host.getGrain(IMaybeGrain, "a3").describeMapValue(byName)).toBe(
        "undefined",
      );
    } finally {
      await cluster.dispose();
    }
  });
});
