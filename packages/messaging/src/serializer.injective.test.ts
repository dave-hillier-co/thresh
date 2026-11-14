import { describe, expect, it } from "vitest";
import { encode } from "@msgpack/msgpack";
import { JsonSerializer } from "@thresh/messaging/json-serializer";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import type { Serializer } from "@thresh/messaging/serializer";

const hostile = (): unknown =>
  JSON.parse('{"__proto__": {"polluted": true}, "constructor": 1, "prototype": 2, "ok": 3}');

const fixtures: Array<[string, () => unknown]> = [
  ["tag key naming a real tag", () => ({ $thresh: "date", value: 5 })],
  ["nested bigint-shaped object", () => ({ note: { $thresh: "bigint", value: "abc" } })],
  ["undefined-shaped object in an array", () => [{ $thresh: "undefined" }, 1]],
  ["non-string tag", () => ({ $thresh: 7 })],
  ["set-shaped object", () => ({ $thresh: "set", values: [1, 2] })],
  ["escape-envelope-shaped object", () => ({ $thresh: "plainObject", entries: [["a", 1]] })],
  ["object with the dangerous keys as own data", hostile],
];

function describeShape(value: unknown): unknown {
  if (value === null || typeof value !== "object") return value;
  if (value instanceof Map) {
    return ["Map", [...value].map(([k, v]) => [describeShape(k), describeShape(v)])];
  }
  if (value instanceof Set) return ["Set", [...value].map(describeShape)];
  if (Array.isArray(value)) return ["Array", value.map(describeShape)];
  return [
    Object.getPrototypeOf(value) === Object.prototype ? "Object" : "OtherProto",
    Object.getOwnPropertyNames(value).map((k) => [
      k,
      describeShape(Object.getOwnPropertyDescriptor(value, k)?.value),
    ]),
  ];
}

describe.each<[string, Serializer]>([
  ["MessagePackSerializer", new MessagePackSerializer()],
  ["JsonSerializer", new JsonSerializer()],
])("%s is injective over plain data", (_name, s) => {
  describe.each(fixtures)("%s", (_label, make) => {
    it("round-trips on its own", () => {
      expect(describeShape(s.deserialize(s.serialize(make())))).toEqual(describeShape(make()));
    });

    it("round-trips nested in a Map, a Set, an array and an error's properties", () => {
      const wrapped = (): unknown => [
        new Map<unknown, unknown>([["k", make()]]),
        new Set([make()]),
        [make()],
        Object.assign(new Error("boom"), { data: make() }),
      ];
      const back = s.deserialize(s.serialize(wrapped())) as unknown[];
      const expected = wrapped() as unknown[];
      expect(describeShape(back.slice(0, 3))).toEqual(describeShape(expected.slice(0, 3)));
      expect(describeShape((back[3] as { data: unknown }).data)).toEqual(describeShape(make()));
    });
  });

  it("decodes hostile wire bytes without touching a prototype", () => {
    const before = Object.getOwnPropertyNames(Object.prototype).sort();
    if (s instanceof MessagePackSerializer) {
      // msgpack's own decoder refuses a `__proto__` map key outright, so a hostile raw payload is
      // rejected before it reaches the codec; honest `__proto__` data travels in the envelope.
      expect(() => s.deserialize(encode(hostile()))).toThrow(/__proto__/);
      return;
    }
    const back = s.deserialize(
      new TextEncoder().encode(
        '{"__proto__":{"polluted":true},"x":{"__proto__":{"polluted":true}}}',
      ),
    ) as Record<string, unknown>;
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
    expect(Object.getOwnPropertyNames(Object.prototype).sort()).toEqual(before);
    expect(Object.getPrototypeOf(back)).toBe(Object.prototype);
  });
});
