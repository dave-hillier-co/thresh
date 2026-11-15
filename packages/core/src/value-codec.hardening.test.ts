import { describe, expect, it } from "vitest";
import { decodeValue, encodeValue, typedArrayConstructor } from "./value-codec";

describe("decodeValue refuses a hostile typed-array kind", () => {
  it.each(["constructor", "toString", "__proto__", "hasOwnProperty", "valueOf"])(
    "kind %s",
    (kind) => {
      expect(typedArrayConstructor(kind)).toBeUndefined();
      expect(() =>
        decodeValue(JSON.parse(`{"$thresh":"typedArray","kind":"${kind}","values":[1]}`)),
      ).toThrow(/unknown typed array kind/);
    },
  );

  it("refuses a non-string kind", () => {
    expect(() => decodeValue({ $thresh: "typedArray", kind: {}, values: [] })).toThrow(
      /unknown typed array kind/,
    );
  });
});

describe("decodeValue refuses a hostile grain key kind", () => {
  it.each(["constructor", "toString", "__proto__", "nope"])("keyKind %s", (keyKind) => {
    expect(() => decodeValue({ $thresh: "grainId", grainType: "A", key: "k", keyKind })).toThrow(
      /unknown grain key kind/,
    );
  });
});

describe("in-memory encode and decode agree with the transports on arrays", () => {
  class Items extends Array<number> {}
  it("flattens an Array subclass", () => {
    const out = decodeValue(encodeValue(Items.from([1, 2]))) as unknown[];
    expect(Object.getPrototypeOf(out)).toBe(Array.prototype);
    expect(Object.getPrototypeOf(encodeValue(Items.from([1])))).toBe(Array.prototype);
    expect(Object.getPrototypeOf(decodeValue(Items.from([1])))).toBe(Array.prototype);
  });
});

describe("in-memory decode of bytes", () => {
  it("returns a fresh exact buffer", () => {
    const source = new Uint8Array(new Uint8Array([9, 8, 7, 6]).buffer, 1, 2);
    const out = decodeValue(source) as Uint8Array;
    expect(out.buffer.byteLength).toBe(2);
    expect(Array.from(out)).toEqual([8, 7]);
  });
});
