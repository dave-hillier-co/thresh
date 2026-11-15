import { describe, expect, it } from "vitest";
import { immutable, isImmutable } from "@thresh/core/immutable";
import {
  deserializeValue,
  encodeValue,
  serializeValue,
  UnsupportedValueError,
} from "@thresh/core/value-codec";
import { copyValue } from "@thresh/core/value-copier";

class Snapshot {
  constructor(readonly rows: number[]) {}
  total(): number {
    return this.rows.reduce((a, b) => a + b, 0);
  }
}

describe("immutable", () => {
  it("returns the value itself, with its shape unchanged", () => {
    const value = { a: 1, b: [2] };
    const marked = immutable(value);
    expect(marked).toBe(value);
    expect(Object.keys(marked)).toEqual(["a", "b"]);
    expect(Reflect.ownKeys(marked)).toEqual(["a", "b"]);
    expect(isImmutable(value)).toBe(true);
  });

  it("marks nothing it was not given", () => {
    const inner = { x: 1 };
    immutable({ inner });
    expect(isImmutable(inner)).toBe(false);
    expect(isImmutable({})).toBe(false);
  });

  it("hands a primitive back unchanged: it needs no mark", () => {
    expect(immutable(5)).toBe(5);
    expect(immutable("s")).toBe("s");
    expect(immutable(undefined)).toBeUndefined();
  });

  it("cannot mark a function, which the copier still refuses", () => {
    const fn = (): number => 1;
    expect(immutable(fn)).toBe(fn);
    expect(isImmutable(fn)).toBe(false);
    expect(() => copyValue([fn])).toThrow(UnsupportedValueError);
  });
});

describe("the copier and a marked value", () => {
  it("returns a marked value itself, uncopied, on its own", () => {
    const value = immutable(new Snapshot([1, 2]));
    expect(copyValue(value)).toBe(value);
  });

  it("returns a marked value itself at any depth", () => {
    const shared = immutable({ rows: [1, 2, 3] });
    const bytes = immutable(new Uint8Array([1, 2]));
    const copy = copyValue({
      list: [shared],
      map: new Map([["k", shared]]),
      set: new Set([shared]),
      deep: { deeper: { bytes } },
    });
    expect(copy.list[0]).toBe(shared);
    expect(copy.map.get("k")).toBe(shared);
    expect([...copy.set][0]).toBe(shared);
    expect(copy.deep.deeper.bytes).toBe(bytes);
  });

  it("still copies what surrounds a marked value", () => {
    const shared = immutable({ n: 1 });
    const outer = { shared, other: { n: 2 } };
    const copy = copyValue(outer);
    expect(copy).not.toBe(outer);
    expect(copy.other).not.toBe(outer.other);
    expect(copy.shared).toBe(shared);
  });

  it("keeps a marked class instance's class, where an unmarked one is flattened", () => {
    expect(copyValue(immutable(new Snapshot([4]))).total()).toBe(4);
    expect(copyValue(new Snapshot([4]))).not.toBeInstanceOf(Snapshot);
  });
});

describe("the codec and a marked value", () => {
  it("ignores the mark, so a cross-silo call is unaffected", () => {
    const value = { a: 1, at: new Date(5) };
    const plain = encodeValue({ a: 1, at: new Date(5) });
    expect(encodeValue(immutable(value))).toEqual(plain);
    const back = deserializeValue<typeof value>(serializeValue(value));
    expect(back).not.toBe(value);
    expect(back).toEqual(value);
  });

  it("still flattens a marked class instance on the wire", () => {
    const back = deserializeValue(serializeValue(immutable(new Snapshot([1]))));
    expect(back).not.toBeInstanceOf(Snapshot);
    expect(back).toEqual({ rows: [1] });
  });
});
