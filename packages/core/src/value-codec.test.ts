import { afterEach, describe, expect, it } from "vitest";
import {
  CircularReferenceError,
  clearSurrogates,
  decodeValue,
  deserializeValue,
  encodeValue,
  registerSurrogate,
  serializeValue,
  unregisterSurrogate,
  type SurrogateDescriptor,
} from "@thresh/core/value-codec";
import { GrainId } from "@thresh/core/grain-id";

describe("value-codec", () => {
  afterEach(() => {
    clearSurrogates();
  });

  describe("built-in types", () => {
    it("round-trips a Date", () => {
      const date = new Date("2026-07-24T12:00:00.000Z");
      expect(decodeValue(encodeValue(date))).toEqual(date);
    });

    it("round-trips a bigint", () => {
      expect(decodeValue(encodeValue(123456789012345678901234567890n))).toBe(
        123456789012345678901234567890n,
      );
    });

    it("round-trips a Map, including non-string keys", () => {
      const map = new Map<unknown, unknown>([
        ["a", 1],
        [2, "b"],
        [new Date(0), "epoch"],
      ]);
      expect(decodeValue(encodeValue(map))).toEqual(map);
    });

    it("round-trips a Set", () => {
      const set = new Set([1, "two", new Date(0)]);
      expect(decodeValue(encodeValue(set))).toEqual(set);
    });

    it("round-trips a Set nested inside a plain object and an array", () => {
      const value = { tags: new Set(["a", "b"]), all: [new Set([1, 2])] };
      expect(decodeValue(encodeValue(value))).toEqual(value);
    });

    it("round-trips a TOP-LEVEL undefined, so an absent return value stays absent", () => {
      // The shape a grain method declared `Promise<T | undefined>` returns when it has nothing.
      // Neither transport can carry a bare `undefined` (MessagePack writes nil; `JSON.stringify`
      // yields no string at all), so it is tagged — otherwise the caller reads `null` and every
      // `=== undefined` guard downstream fails open.
      expect(decodeValue(encodeValue(undefined))).toBeUndefined();
    });

    it("round-trips a top-level null as null, distinct from undefined", () => {
      expect(decodeValue(encodeValue(null))).toBeNull();
    });

    it("leaves a NESTED undefined member as an omitted key, not a tagged envelope", () => {
      // Deliberate asymmetry: only the top level is tagged. An object's optional field must keep
      // travelling as an absent key, or every message grows and "key absent" becomes "key present
      // with an undefined value".
      const encoded = encodeValue({ a: 1, b: undefined }) as Record<string, unknown>;
      expect(encoded.b).toBeUndefined();
      expect((decodeValue(encoded) as Record<string, unknown>).b).toBeUndefined();
    });

    it("round-trips a GrainId", () => {
      const id = new GrainId("Counter", "tenant-42");
      const decoded = decodeValue(encodeValue(id)) as GrainId;
      expect(decoded.equals(id)).toBe(true);
    });
  });

  describe("binary", () => {
    // `encodeValue` passes a `Uint8Array` through untouched, which is right for the MessagePack
    // path (msgpack has a native binary type) and for the in-memory clone. The JSON path has no
    // such type: `JSON.stringify` turns a typed array into `{"0":1,...}` and `JSON.parse` hands
    // back a plain object, so every byte array in a durably persisted grain state silently stopped
    // being a `Uint8Array`. `serializeValue`/`deserializeValue` therefore tag binary as base64.
    it("round-trips a Uint8Array through serializeValue/deserializeValue", () => {
      const bytes = new Uint8Array([0, 1, 2, 253, 254, 255]);

      const back = deserializeValue<Uint8Array>(serializeValue(bytes));

      expect(back).toBeInstanceOf(Uint8Array);
      expect([...back]).toEqual([...bytes]);
    });

    it("round-trips an empty Uint8Array, and one whose length is not a multiple of three", () => {
      for (const bytes of [new Uint8Array([]), new Uint8Array([1]), new Uint8Array([1, 2])]) {
        const back = deserializeValue<Uint8Array>(serializeValue(bytes));
        expect(back).toBeInstanceOf(Uint8Array);
        expect([...back]).toEqual([...bytes]);
      }
    });

    it("round-trips a Uint8Array nested in an object, an array and a Map value", () => {
      const value = {
        schema: new TextEncoder().encode("definition doc {}"),
        history: [new Uint8Array([9, 8])],
        byName: new Map<string, Uint8Array>([["a", new Uint8Array([7])]]),
      };

      const back = deserializeValue<typeof value>(serializeValue(value));

      expect([...back.schema]).toEqual([...value.schema]);
      expect(back.schema).toBeInstanceOf(Uint8Array);
      expect([...(back.history[0] ?? [])]).toEqual([9, 8]);
      expect(back.byName.get("a")).toBeInstanceOf(Uint8Array);
    });

    it("round-trips every byte value, so the base64 alphabet is exercised end to end", () => {
      const all = new Uint8Array(256);
      for (let i = 0; i < 256; i++) all[i] = i;

      expect([...deserializeValue<Uint8Array>(serializeValue(all))]).toEqual([...all]);
    });

    it("leaves binary as a live Uint8Array for the MessagePack and clone paths", () => {
      // `encodeValue` with no options is the native-binary path: msgpack carries a `Uint8Array`
      // itself, so tagging it as base64 there would cost a third of every message body.
      const bytes = new Uint8Array([1, 2, 3]);

      expect(encodeValue(bytes)).toBe(bytes);
      expect(decodeValue(encodeValue(bytes))).toBe(bytes);
    });
  });

  describe("wire compatibility", () => {
    it("decodes an envelope with no version field (the pre-existing wire shape)", () => {
      const legacy = { $thresh: "date", value: 0 };
      expect(decodeValue(legacy)).toEqual(new Date(0));
    });

    it("stamps a version field on freshly encoded envelopes", () => {
      const encoded = encodeValue(new Date(0)) as Record<string, unknown>;
      expect(encoded.$tsvv).toBe(1);
    });

    it("still decodes freshly encoded envelopes (round-trip through the version field)", () => {
      const date = new Date(0);
      expect(decodeValue(encodeValue(date))).toEqual(date);
    });

    it("decodes an unknown tag as a plain object instead of throwing", () => {
      const fromTheFuture = { $thresh: "some-future-type", $tsvv: 2, value: 1 };
      expect(decodeValue(fromTheFuture)).toEqual(fromTheFuture);
    });
  });

  describe("surrogate registration", () => {
    class Money {
      constructor(
        public readonly cents: number,
        public readonly currency: string,
      ) {}
    }

    const moneySurrogate: SurrogateDescriptor<Money> = {
      tag: "money",
      test: (v): v is Money => v instanceof Money,
      encode: (v) => ({ cents: v.cents, currency: v.currency }),
      decode: (fields) => new Money(fields.cents as number, fields.currency as string),
    };

    it("encodes and decodes a registered user type by its tag", () => {
      registerSurrogate(moneySurrogate);
      const money = new Money(1050, "GBP");
      const decoded = decodeValue(encodeValue(money)) as Money;
      expect(decoded).toBeInstanceOf(Money);
      expect(decoded).toEqual(money);
    });

    it("round-trips a registered type through serializeValue/deserializeValue", () => {
      registerSurrogate(moneySurrogate);
      const money = new Money(500, "USD");
      const json = serializeValue(money);
      expect(deserializeValue<Money>(json)).toEqual(money);
    });

    it("encodes nested values within a surrogate's fields", () => {
      class Receipt {
        constructor(
          public readonly issuedAt: Date,
          public readonly total: Money,
        ) {}
      }
      registerSurrogate(moneySurrogate);
      registerSurrogate<Receipt>({
        tag: "receipt",
        test: (v): v is Receipt => v instanceof Receipt,
        encode: (v) => ({ issuedAt: v.issuedAt, total: v.total }),
        decode: (fields) => new Receipt(fields.issuedAt as Date, fields.total as Money),
      });

      const receipt = new Receipt(new Date("2026-01-01T00:00:00.000Z"), new Money(100, "GBP"));
      const decoded = decodeValue(encodeValue(receipt)) as Receipt;
      expect(decoded).toEqual(receipt);
      expect(decoded.total).toBeInstanceOf(Money);
    });

    it("rejects registering the same tag twice", () => {
      registerSurrogate(moneySurrogate);
      expect(() => registerSurrogate(moneySurrogate)).toThrow(/already registered/);
    });

    it("stops encoding under a tag once unregistered", () => {
      registerSurrogate(moneySurrogate);
      unregisterSurrogate("money");
      const money = new Money(1, "GBP");
      // No surrogate left to claim it, and it is not one of the built-ins,
      // so it falls through to the plain-object branch.
      expect(encodeValue(money)).toEqual({ cents: 1, currency: "GBP" });
    });

    describe("polymorphism resolution", () => {
      abstract class Shape {}
      class Circle extends Shape {
        constructor(public readonly radius: number) {
          super();
        }
      }
      class Square extends Shape {
        constructor(public readonly side: number) {
          super();
        }
      }

      it("decodes distinct subclasses registered under distinct tags back to their concrete constructor", () => {
        registerSurrogate<Circle>({
          tag: "circle",
          test: (v): v is Circle => v instanceof Circle,
          encode: (v) => ({ radius: v.radius }),
          decode: (fields) => new Circle(fields.radius as number),
        });
        registerSurrogate<Square>({
          tag: "square",
          test: (v): v is Square => v instanceof Square,
          encode: (v) => ({ side: v.side }),
          decode: (fields) => new Square(fields.side as number),
        });

        const shapes: Shape[] = [new Circle(3), new Square(4)];
        const decoded = decodeValue(encodeValue(shapes)) as Shape[];
        expect(decoded[0]).toBeInstanceOf(Circle);
        expect((decoded[0] as Circle).radius).toBe(3);
        expect(decoded[1]).toBeInstanceOf(Square);
        expect((decoded[1] as Square).side).toBe(4);
      });
    });
  });

  describe("circular-reference guard", () => {
    it("throws CircularReferenceError for a self-referencing plain object instead of overflowing the stack", () => {
      const obj: Record<string, unknown> = { name: "root" };
      obj.self = obj;
      expect(() => encodeValue(obj)).toThrow(CircularReferenceError);
    });

    it("throws CircularReferenceError for a self-referencing array", () => {
      const arr: unknown[] = [1, 2];
      arr.push(arr);
      expect(() => encodeValue(arr)).toThrow(CircularReferenceError);
    });

    it("throws CircularReferenceError for a cycle through a Map", () => {
      const map = new Map<string, unknown>();
      map.set("self", map);
      expect(() => encodeValue(map)).toThrow(CircularReferenceError);
    });

    it("throws CircularReferenceError for a cycle spanning nested objects", () => {
      const a: Record<string, unknown> = {};
      const b: Record<string, unknown> = { a };
      a.b = b;
      expect(() => encodeValue(a)).toThrow(CircularReferenceError);
    });

    it("does not throw for the same object referenced twice without a cycle (a DAG, not a cycle)", () => {
      const shared = { value: 1 };
      const value = { left: shared, right: shared };
      expect(() => encodeValue(value)).not.toThrow();
      expect(decodeValue(encodeValue(value))).toEqual(value);
    });
  });
});
