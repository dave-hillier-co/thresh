import { afterEach, describe, expect, it } from "vitest";
import {
  decodeValue,
  encodeValue,
  registerSurrogate,
  unregisterSurrogate,
  UnsupportedValueError,
} from "@thresh/core/value-codec";
import { JsonSerializer } from "@thresh/messaging/json-serializer";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import type { Serializer } from "@thresh/messaging/serializer";

const transports: Array<[string, () => Serializer]> = [
  ["MessagePack", () => new MessagePackSerializer()],
  ["JSON", () => new JsonSerializer()],
];

function roundTrip(serializer: Serializer, value: unknown): unknown {
  return serializer.deserialize(serializer.serialize(value));
}

class Probe {
  constructor(readonly fields: Record<string, unknown>) {}
}
const probeSurrogate = {
  tag: "test.hardening.probe",
  test: (v: unknown) => v instanceof Probe,
  encode: (p: Probe) => p.fields,
  decode: (f: Record<string, unknown>) => new Probe(f),
};

afterEach(() => unregisterSurrogate(probeSurrogate.tag));

describe.each(transports)("%s hardening", (_name, make) => {
  it("carries NaN and the infinities as themselves in every position", () => {
    const s = make();
    for (const n of [Number.NaN, Infinity, -Infinity]) {
      expect(roundTrip(s, n)).toBe(n);
      expect(roundTrip(s, { n })).toEqual({ n });
      expect((roundTrip(s, new Date(n)) as Date).getTime()).toBe(Number.NaN);
      expect(Array.from(roundTrip(s, new Float64Array([1, n, -0])) as Float64Array)).toEqual([
        1,
        n,
        -0,
      ]);
      expect(Array.from(roundTrip(s, new Float32Array([n, 2])) as Float32Array)).toEqual([n, 2]);
    }
  });

  it("round-trips a surrogate whose fields collide with the envelope", () => {
    registerSurrogate(probeSurrogate);
    const before = Object.getOwnPropertyNames(Object.prototype).sort();
    const fields: Record<string, unknown> = {};
    for (const [k, v] of [
      ["$thresh", "date"],
      ["$tsvv", 99],
      ["__proto__", { polluted: true }],
      ["constructor", "c"],
      ["prototype", [1]],
      ["ok", 1],
    ] as const) {
      Object.defineProperty(fields, k, {
        value: v,
        enumerable: true,
        writable: true,
        configurable: true,
      });
    }
    const out = roundTrip(make(), new Probe(fields)) as Probe;
    expect(out).toBeInstanceOf(Probe);
    expect(Object.keys(out.fields)).toEqual(Object.keys(fields));
    for (const k of Object.keys(fields)) {
      expect(Object.getOwnPropertyDescriptor(out.fields, k)?.value).toEqual(
        Object.getOwnPropertyDescriptor(fields, k)?.value,
      );
    }
    expect(Object.getPrototypeOf(out.fields)).toBe(Object.prototype);
    expect(Object.getOwnPropertyNames(Object.prototype).sort()).toEqual(before);
    expect(({} as Record<string, unknown>).polluted).toBeUndefined();
  });

  it.each([10, 300, 5000, 70000])("treats a lone surrogate in a string of %i the same way", (n) => {
    const s = make();
    for (const lone of ["\uD800", "\uDC00"]) {
      const text = "x".repeat(n - 1) + lone;
      expect(() => s.serialize(text)).toThrow(UnsupportedValueError);
      expect(() => s.serialize({ nested: [text] })).toThrow(UnsupportedValueError);
      expect(() => s.serialize({ [text]: 1 })).toThrow(UnsupportedValueError);
    }
    const paired = "x".repeat(n - 2) + "😀";
    expect(roundTrip(s, paired)).toBe(paired);
  });

  it("returns an exact fresh buffer for a decoded Uint8Array", () => {
    const s = make();
    const bytes = s.serialize([new Uint8Array([1, 2, 3]), new Uint8Array([4, 5, 6])]);
    const [a, b] = s.deserialize<Uint8Array[]>(bytes) as [Uint8Array, Uint8Array];
    expect(a.buffer.byteLength).toBe(3);
    expect(a.byteOffset).toBe(0);
    a[0] = 99;
    expect(Array.from(b)).toEqual([4, 5, 6]);
    expect(Array.from(s.deserialize<Uint8Array[]>(bytes)[0] as Uint8Array)).toEqual([1, 2, 3]);
  });

  it("flattens an Array subclass", () => {
    class Items extends Array<number> {}
    const out = roundTrip(make(), [Items.from([1, 2])]) as unknown[];
    expect(Object.getPrototypeOf(out[0])).toBe(Array.prototype);
    const top = roundTrip(make(), Items.from([1, 2]));
    expect(Object.getPrototypeOf(top)).toBe(Array.prototype);
  });
});

describe("payloads written before the hardening still decode", () => {
  it("a JSON date envelope", () => {
    const d = decodeValue(JSON.parse('{"$thresh":"date","$tsvv":1,"value":5}')) as Date;
    expect(d.getTime()).toBe(5);
  });
  it("a JSON typed array with a negative zero", () => {
    const v = decodeValue(
      JSON.parse(
        '{"$thresh":"typedArray","$tsvv":1,"kind":"Float64Array","values":[1.5,{"$thresh":"negZero","$tsvv":1}]}',
      ),
    ) as Float64Array;
    expect(Array.from(v)).toEqual([1.5, -0]);
  });
  it("a flat surrogate envelope", () => {
    registerSurrogate(probeSurrogate);
    const v = decodeValue(
      JSON.parse('{"$thresh":"test.hardening.probe","$tsvv":1,"cents":5,"currency":"GBP"}'),
    ) as Probe;
    expect(v.fields).toEqual({ cents: 5, currency: "GBP" });
  });
  it("a MessagePack bare NaN float64 and a long string", () => {
    const nan = new MessagePackSerializer().deserialize(
      Uint8Array.from([0xcb, 0x7f, 0xf8, 0, 0, 0, 0, 0, 0]),
    );
    expect(nan).toBeNaN();
    const text = "y".repeat(300);
    const bytes = new MessagePackSerializer().serialize(text);
    expect(new MessagePackSerializer().deserialize(bytes)).toBe(text);
  });
  it("a surrogate with no collisions keeps its flat wire shape", () => {
    registerSurrogate(probeSurrogate);
    expect(encodeValue(new Probe({ cents: 5 }))).toEqual({
      $thresh: "test.hardening.probe",
      $tsvv: 1,
      cents: 5,
    });
  });
});
