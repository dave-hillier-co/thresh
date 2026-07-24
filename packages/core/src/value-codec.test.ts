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
} from "@tsva/core/value-codec";
import { GrainId } from "@tsva/core/grain-id";
import { Guid } from "@tsva/core/guid";

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

    it("round-trips a GrainId", () => {
      const id = new GrainId("Counter", "tenant-42");
      const decoded = decodeValue(encodeValue(id)) as GrainId;
      expect(decoded.equals(id)).toBe(true);
    });
  });

  describe("wire compatibility", () => {
    it("decodes an envelope with no version field (the pre-existing wire shape)", () => {
      const legacy = { $tsva: "date", value: 0 };
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
      const fromTheFuture = { $tsva: "some-future-type", $tsvv: 2, value: 1 };
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
