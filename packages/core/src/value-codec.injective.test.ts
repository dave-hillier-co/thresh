import { describe, expect, it } from "vitest";
import { UnavailableExceptionFallbackException } from "@thresh/core/errors";
import {
  decodeValue,
  deserializeValue,
  encodeValue,
  serializeValue,
} from "@thresh/core/value-codec";

const hostile = (): unknown =>
  JSON.parse('{"__proto__": {"polluted": true}, "constructor": 1, "prototype": 2, "ok": 3}');

/** The shape the escape envelope takes on the wire: user data shaped exactly like it must survive too. */
const envelopeShaped = (): unknown => ({ $thresh: "plainObject", entries: [["a", 1]] });

const fixtures: Array<[string, () => unknown]> = [
  ["tag key naming a real tag", () => ({ $thresh: "date", value: 5 })],
  ["nested bigint-shaped object", () => ({ note: { $thresh: "bigint", value: "abc" } })],
  ["undefined-shaped object in an array", () => [{ $thresh: "undefined" }, 1]],
  ["non-string tag", () => ({ $thresh: 7 })],
  ["set-shaped object", () => ({ $thresh: "set", values: [1, 2] })],
  ["escape-envelope-shaped object", envelopeShaped],
  ["object with the dangerous keys as own data", hostile],
  ["tag key with a version stamp", () => ({ $thresh: "date", $tsvv: 99, value: 1 })],
  ["explicit undefined member beside a tag key", () => ({ $thresh: "x", gone: undefined })],
];

function describeShape(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (value instanceof Map) {
    return ["Map", [...value].map(([k, v]) => [describeShape(k), describeShape(v)])];
  }
  if (value instanceof Set) return ["Set", [...value].map(describeShape)];
  if (Array.isArray(value)) return ["Array", value.map(describeShape)];
  const proto = Object.getPrototypeOf(value);
  return [
    proto === Object.prototype ? "Object" : "OtherProto",
    Object.getOwnPropertyNames(value).map((k) => [
      k,
      describeShape(Object.getOwnPropertyDescriptor(value, k)?.value),
    ]),
  ];
}

const transports: Array<[string, (v: unknown) => unknown]> = [
  ["tagged JSON", (v) => deserializeValue(serializeValue(v))],
  [
    "encode/decode through JSON text",
    (v) => decodeValue(JSON.parse(JSON.stringify(encodeValue(v)))),
  ],
];

function expectIdentity(back: unknown, original: unknown): void {
  expect(describeShape(back)).toEqual(describeShape(original));
}

describe("the codec is injective over plain data", () => {
  describe.each(transports)("%s", (_name, roundTrip) => {
    describe.each(fixtures)("%s", (_label, make) => {
      it("round-trips on its own", () => {
        expectIdentity(roundTrip(make()), make());
      });

      it("round-trips inside a Map value and key, a Set member and an array", () => {
        const wrapped = (): unknown => [
          new Map<unknown, unknown>([
            ["k", make()],
            [make(), 1],
          ]),
          new Set([make()]),
          [make()],
          { deep: { deeper: make() } },
        ];
        expectIdentity(roundTrip(wrapped()), wrapped());
      });

      it("round-trips inside an error's properties", () => {
        const withError = (): Error => Object.assign(new Error("boom"), { data: make() });
        const back = roundTrip(withError()) as Record<string, unknown>;
        expect(back).toBeInstanceOf(Error);
        expectIdentity(back.data, make());
      });
    });
  });

  it("keeps the dangerous keys as own data properties without touching a prototype", () => {
    const back = deserializeValue<Record<string, unknown>>(serializeValue(hostile()));
    expect(Object.getPrototypeOf(back)).toBe(Object.prototype);
    expect(Object.keys(back).sort()).toEqual(["__proto__", "constructor", "ok", "prototype"]);
    expect(Object.getOwnPropertyDescriptor(back, "__proto__")?.value).toEqual({ polluted: true });
    expect(back.constructor).toBe(1);
    expect((back as { polluted?: boolean }).polluted).toBeUndefined();
  });

  it("keeps an error property named __proto__ as own data", () => {
    const error = new Error("boom");
    Object.defineProperty(error, "__proto__", {
      value: { polluted: true },
      enumerable: true,
      configurable: true,
      writable: true,
    });
    const back = deserializeValue<Error>(serializeValue(error));
    expect(Object.getPrototypeOf(back)).toBe(UnavailableExceptionFallbackException.prototype);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });

  describe("hostile input that is not an envelope", () => {
    const wire =
      '{"a":{"__proto__":{"polluted":true},"b":[{"constructor":{"prototype":{"polluted":true}}}]},' +
      '"__proto__":{"polluted":true},"m":{"$thresh":"map","entries":[["k",{"__proto__":{"polluted":true}}]]}}';

    it("decodes __proto__, constructor and prototype as own data, never by assignment", () => {
      const before = Object.getOwnPropertyNames(Object.prototype).sort();
      const back = deserializeValue<Record<string, any>>(wire);
      expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
      expect(Object.getOwnPropertyNames(Object.prototype).sort()).toEqual(before);
      expect(Object.getPrototypeOf(back)).toBe(Object.prototype);
      expect(Object.getOwnPropertyDescriptor(back, "__proto__")?.value).toEqual({ polluted: true });
      expect(Object.getPrototypeOf(back.a)).toBe(Object.prototype);
      expect(Object.getOwnPropertyDescriptor(back.a, "__proto__")?.value).toEqual({
        polluted: true,
      });
      expect(Object.getPrototypeOf(back.m.get("k"))).toBe(Object.prototype);
    });

    it("decodes the same input from a decoded-JSON object (the msgpack shape)", () => {
      const before = Object.getOwnPropertyNames(Object.prototype).sort();
      const back = decodeValue(JSON.parse(wire)) as Record<string, unknown>;
      expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
      expect(Object.getOwnPropertyNames(Object.prototype).sort()).toEqual(before);
      expect(Object.getPrototypeOf(back)).toBe(Object.prototype);
    });

    it("decodes a hostile error property map as own data", () => {
      const payload = JSON.parse(
        '{"$thresh":"error","name":"X","message":"m","properties":{"__proto__":{"polluted":true},"k":1}}',
      );
      const back = decodeValue(payload) as Error & Record<string, unknown>;
      expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
      expect(Object.getPrototypeOf(back)).toBe(UnavailableExceptionFallbackException.prototype);
      expect(back.k).toBe(1);
    });
  });

  describe("ordinary objects keep today's wire shape", () => {
    it("encodes a representative value with no envelope", () => {
      const encoded = encodeValue({ a: 1, b: { c: "x", d: [1, { e: null }] }, f: new Date(5) });
      expect(encoded).toEqual({
        a: 1,
        b: { c: "x", d: [1, { e: null }] },
        f: { $thresh: "date", $tsvv: 1, value: 5 },
      });
    });
  });

  describe("the escape envelope", () => {
    it("carries a tag-key object's entries as data and stamps the schema version", () => {
      expect(encodeValue({ $thresh: "date", value: 5 })).toEqual({
        $thresh: "plainObject",
        $tsvv: 1,
        entries: [
          ["$thresh", "date"],
          ["value", 5],
        ],
      });
    });

    it("refuses a newer schema version like every other built-in tag", () => {
      expect(() => decodeValue({ $thresh: "plainObject", $tsvv: 2, entries: [] })).toThrow(
        /version/i,
      );
    });

    it("rejects a malformed envelope", () => {
      expect(() => decodeValue({ $thresh: "plainObject", entries: "nope" })).toThrow();
    });
  });
});
