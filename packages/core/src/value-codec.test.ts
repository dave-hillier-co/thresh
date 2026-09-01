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
import {
  GrainCallError,
  isThreshRuntimeError,
  LimitExceededException as ThreshLimitExceededException,
  RejectionError,
  UnavailableExceptionFallbackException,
} from "@thresh/core/errors";

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

  describe("errors without a surrogate", () => {
    /** A domain error the codec has never been taught about -- the shape issue #61 is about. */
    class QuotaExceededError extends Error {
      constructor(
        message: string,
        readonly limit: number,
      ) {
        super(message);
        this.name = "QuotaExceededError";
      }
    }

    it("carries name, message and own enumerable properties for an unregistered Error subclass", () => {
      const decoded = decodeValue(encodeValue(new QuotaExceededError("over the limit", 42)));
      expect(decoded).toBeInstanceOf(Error);
      expect(decoded).toBeInstanceOf(UnavailableExceptionFallbackException);
      const error = decoded as UnavailableExceptionFallbackException;
      expect(error.name).toBe("QuotaExceededError");
      expect(error.errorType).toBe("QuotaExceededError");
      expect(error.message).toBe("over the limit");
      expect(error.properties.limit).toBe(42);
      expect((error as unknown as QuotaExceededError).limit).toBe(42);
    });

    it("round-trips through serializeValue/deserializeValue", () => {
      const decoded = deserializeValue<Error>(serializeValue(new QuotaExceededError("nope", 7)));
      expect(decoded).toBeInstanceOf(UnavailableExceptionFallbackException);
      expect(decoded.name).toBe("QuotaExceededError");
      expect(decoded.message).toBe("nope");
    });

    it("carries a plain Error as an Error rather than flattening it to {}", () => {
      const decoded = decodeValue(encodeValue(new Error("plain")));
      expect(decoded).toBeInstanceOf(Error);
      expect((decoded as Error).name).toBe("Error");
      expect((decoded as Error).message).toBe("plain");
    });

    it("encodes an Error nested inside a plain object and an array", () => {
      const decoded = decodeValue(
        encodeValue({ failures: [new QuotaExceededError("deep", 1)] }),
      ) as { failures: Error[] };
      expect(decoded.failures[0]).toBeInstanceOf(UnavailableExceptionFallbackException);
      expect(decoded.failures[0]!.name).toBe("QuotaExceededError");
    });

    it("rebuilds a Thresh runtime error as its own class, not as the fallback", () => {
      // The runtime's own error family is the analogue of Orleans resolving an exception type it
      // knows: `catch (OrleansException)` keeps working across the wire, so `isThreshRuntimeError`
      // and an `instanceof GrainCallError` narrowing must too.
      const decoded = decodeValue(encodeValue(new GrainCallError("boom")));
      expect(decoded).toBeInstanceOf(GrainCallError);
      expect((decoded as GrainCallError).message).toBe("boom");

      const rejection = decodeValue(encodeValue(new RejectionError("nope", "siloDraining")));
      expect(rejection).toBeInstanceOf(RejectionError);
      expect((rejection as RejectionError).kind).toBe("siloDraining");
    });

    it("lets a registered surrogate win over the generic Error branch", () => {
      registerSurrogate<QuotaExceededError>({
        tag: "test.quota",
        test: (value) => value instanceof QuotaExceededError,
        encode: (error) => ({ message: error.message, limit: error.limit }),
        decode: (fields) =>
          new QuotaExceededError(fields.message as string, fields.limit as number),
      });
      const decoded = decodeValue(encodeValue(new QuotaExceededError("over", 3)));
      expect(decoded).toBeInstanceOf(QuotaExceededError);
      expect(decoded).not.toBeInstanceOf(UnavailableExceptionFallbackException);
    });

    it("throws CircularReferenceError when an error's own property points back at it", () => {
      const error = new Error("self") as Error & { self?: unknown };
      error.self = error;
      expect(() => encodeValue(error)).toThrow(CircularReferenceError);
    });

    it("falls back for an error tag naming a class this process does not know", () => {
      const decoded = decodeValue({
        $thresh: "error",
        $tsvv: 1,
        name: "SomeFutureError",
        message: "from a newer build",
        properties: { code: 9 },
      });
      expect(decoded).toBeInstanceOf(UnavailableExceptionFallbackException);
      expect((decoded as Error).name).toBe("SomeFutureError");
      expect((decoded as UnavailableExceptionFallbackException).properties.code).toBe(9);
    });

    // Orleans keys its rebuild on an assembly-qualified type name, so an application exception can
    // never be mistaken for a framework one. Matching on the bare `name` string can: an
    // application is free to declare a class called `LimitExceededException`, a name Thresh's own
    // errors table uses. Rebuilding THAT as a `ThreshRuntimeError` would make a caller's
    // `isThreshRuntimeError` -- the transliteration of `catch (OrleansException)` -- answer true
    // for a permanent domain failure and retry it.
    it("does not rebuild an application error that merely shares a Thresh error's name", () => {
      class LimitExceededException extends Error {
        constructor(
          message: string,
          readonly quotaName: string,
        ) {
          super(message);
          this.name = "LimitExceededException";
        }
      }

      const decoded = decodeValue(
        encodeValue(new LimitExceededException("tenant over quota", "seats")),
      ) as UnavailableExceptionFallbackException & { quotaName?: string };

      expect(decoded).toBeInstanceOf(UnavailableExceptionFallbackException);
      expect(isThreshRuntimeError(decoded)).toBe(false);
      expect(decoded).not.toBeInstanceOf(ThreshLimitExceededException);
      expect(decoded.name).toBe("LimitExceededException");
      expect(decoded.message).toBe("tenant over quota");
      expect(decoded.quotaName).toBe("seats");
    });

    // Orleans rebuilds the `System.*` namespace with no registration, and upstream leans on it:
    // GrainCallFilterTests catches ArgumentOutOfRangeException BY TYPE on a cross-silo call.
    it("rebuilds JavaScript's built-in error classes as themselves", () => {
      const range = decodeValue(encodeValue(new RangeError("index out of range")));
      expect(range).toBeInstanceOf(RangeError);
      expect((range as RangeError).message).toBe("index out of range");

      const type = decodeValue(encodeValue(new TypeError("not a function")));
      expect(type).toBeInstanceOf(TypeError);
      expect(isThreshRuntimeError(type)).toBe(false);
    });

    it("still rebuilds the real Thresh error of that name as itself", () => {
      const decoded = decodeValue(
        encodeValue(new ThreshLimitExceededException("seats", 11, 10)),
      ) as ThreshLimitExceededException;

      expect(decoded).toBeInstanceOf(ThreshLimitExceededException);
      expect(isThreshRuntimeError(decoded)).toBe(true);
      expect(decoded.limitName).toBe("seats");
      expect(decoded.currentValue).toBe(11);
      expect(decoded.maxValue).toBe(10);
    });
  });

  describe("error cause, stack and AggregateError", () => {
    it("round-trips a cause, rebuilt as its own class and installed non-enumerably", () => {
      const decoded = decodeValue(
        encodeValue(new Error("outer", { cause: new RangeError("inner") })),
      ) as Error;

      expect(decoded.cause).toBeInstanceOf(RangeError);
      expect((decoded.cause as RangeError).message).toBe("inner");
      expect(Object.prototype.propertyIsEnumerable.call(decoded, "cause")).toBe(false);
    });

    it("round-trips a three-deep cause chain, each link rebuilt as its own class", () => {
      const root = new TypeError("root");
      const middle = new GrainCallError("middle", { cause: root });
      const outer = new Error("outer", { cause: middle });

      const decoded = decodeValue(encodeValue(outer)) as Error;

      expect(decoded.cause).toBeInstanceOf(GrainCallError);
      const decodedMiddle = decoded.cause as GrainCallError;
      expect(decodedMiddle.cause).toBeInstanceOf(TypeError);
      expect((decodedMiddle.cause as TypeError).message).toBe("root");
    });

    it("round-trips a non-Error cause (a plain object, and a string)", () => {
      const withObject = decodeValue(
        encodeValue(new Error("outer", { cause: { reason: "quota" } })),
      ) as Error;
      expect(withObject.cause).toEqual({ reason: "quota" });

      const withString = decodeValue(encodeValue(new Error("outer", { cause: "boom" }))) as Error;
      expect(withString.cause).toBe("boom");
    });

    it("keeps an explicit `cause: undefined` as a present own property, not an absent one", () => {
      const decoded = decodeValue(encodeValue(new Error("outer", { cause: undefined }))) as Error;
      expect("cause" in decoded).toBe(true);
      expect(decoded.cause).toBeUndefined();
    });

    it("rebuilds a GrainCallError with its cause, not discarding it a second time", () => {
      const decoded = decodeValue(
        encodeValue(new GrainCallError("outer", { cause: new RangeError("root") })),
      ) as GrainCallError;

      expect(decoded).toBeInstanceOf(GrainCallError);
      expect(decoded.cause).toBeInstanceOf(RangeError);
      expect((decoded.cause as RangeError).message).toBe("root");
    });

    it("installs the sender's stack as a remote stack trace with an end-of-remote-stack marker", () => {
      function throwFromSender(): never {
        throw new Error("boom");
      }
      let sent: Error;
      try {
        throwFromSender();
        throw new Error("unreachable");
      } catch (e) {
        sent = e as Error;
      }

      const decoded = decodeValue(encodeValue(sent)) as Error;

      expect(decoded.stack).toContain("throwFromSender");
      expect(decoded.stack).toContain("--- End of remote stack trace from grain call ---");
    });

    it("caps an oversized stack at STACK_TRACE_CAP with a truncation marker", () => {
      const error = new Error("boom");
      error.stack = "x".repeat(20_000);

      const encoded = encodeValue(error) as Record<string, unknown>;
      const stack = encoded.stack as string;

      expect(stack.length).toBeLessThanOrEqual(8192 + "... (truncated)".length);
      expect(stack.endsWith("... (truncated)")).toBe(true);
    });

    it("round-trips an AggregateError, rebuilding it as itself with its member errors", () => {
      const decoded = decodeValue(
        encodeValue(new AggregateError([new RangeError("a"), new Error("b")], "many")),
      ) as AggregateError;

      expect(decoded).toBeInstanceOf(AggregateError);
      expect(decoded.message).toBe("many");
      expect(decoded.errors[0]).toBeInstanceOf(RangeError);
      expect(decoded.errors[0].message).toBe("a");
      expect(decoded.errors[1]).toBeInstanceOf(Error);
      expect(decoded.errors[1].message).toBe("b");
    });

    it("keeps the decoded errors array on the fallback for an AggregateError subclass", () => {
      class ManyErrors extends AggregateError {
        constructor(errors: unknown[], message: string) {
          super(errors, message);
          this.name = "ManyErrors";
        }
      }

      const decoded = decodeValue(
        encodeValue(new ManyErrors([new RangeError("a")], "many")),
      ) as UnavailableExceptionFallbackException & { errors?: unknown[] };

      expect(decoded).toBeInstanceOf(UnavailableExceptionFallbackException);
      expect(decoded.errors).toBeDefined();
      expect((decoded.errors as Error[])[0]).toBeInstanceOf(RangeError);
    });

    it("throws CircularReferenceError when an error's cause points back at itself", () => {
      const error = new Error("self");
      Object.defineProperty(error, "cause", {
        value: error,
        configurable: true,
        enumerable: false,
      });
      expect(() => encodeValue(error)).toThrow(CircularReferenceError);
    });

    it("round-trips cause, stack and AggregateError.errors through serializeValue/deserializeValue", () => {
      const original = new GrainCallError("outer", { cause: new RangeError("root") });
      const decoded = deserializeValue<GrainCallError>(serializeValue(original));

      expect(decoded).toBeInstanceOf(GrainCallError);
      expect(decoded.cause).toBeInstanceOf(RangeError);
      expect(decoded.stack).toContain("--- End of remote stack trace from grain call ---");

      const aggDecoded = deserializeValue<AggregateError>(
        serializeValue(new AggregateError([new RangeError("a")], "many")),
      );
      expect(aggDecoded).toBeInstanceOf(AggregateError);
      expect(aggDecoded.errors[0]).toBeInstanceOf(RangeError);
    });

    it("still decodes an old envelope with no cause/stack/errors keys exactly as before", () => {
      const decoded = decodeValue({
        $thresh: "error",
        $tsvv: 1,
        name: "Error",
        message: "legacy",
      }) as Error;

      expect(decoded).toBeInstanceOf(Error);
      expect(decoded.message).toBe("legacy");
      expect(decoded.cause).toBeUndefined();
      expect(decoded.stack).toBeDefined();
    });

    it("round-trips an own enumerable `errors` property on a NON-AggregateError (ajv-style validation errors)", () => {
      const error = new Error("invalid") as Error & { errors: unknown[] };
      error.errors = ["must be a string", "must not be empty"];

      const decoded = decodeValue(encodeValue(error)) as UnavailableExceptionFallbackException & {
        errors?: unknown[];
      };

      expect(decoded).toBeInstanceOf(UnavailableExceptionFallbackException);
      expect(decoded.errors).toEqual(["must be a string", "must not be empty"]);
    });

    it("still lands an enumerable `cause`/`errors` carried in an old encoder's `properties` bag onto the rebuilt fallback instance", () => {
      // Shape an old encoder (pre-issue-#63) would have produced: no dedicated top-level `cause`
      // or `errors` fields, both smuggled into `properties` as ordinary enumerable own properties,
      // exactly what `Object.entries` saw before the dedicated handling existed.
      const decoded = decodeValue({
        $thresh: "error",
        $tsvv: 1,
        name: "LegacyError",
        message: "legacy",
        properties: { cause: "root cause", errors: ["a", "b"] },
      }) as UnavailableExceptionFallbackException & { cause?: unknown; errors?: unknown };

      expect(decoded).toBeInstanceOf(UnavailableExceptionFallbackException);
      expect(decoded.cause).toBe("root cause");
      expect(decoded.errors).toEqual(["a", "b"]);
    });

    it("does not duplicate an enumerable own `cause` into properties", () => {
      const error = new Error("outer") as Error & { cause: unknown };
      Object.defineProperty(error, "cause", {
        value: new RangeError("inner"),
        enumerable: true,
        configurable: true,
        writable: true,
      });

      const encoded = encodeValue(error) as Record<string, unknown>;
      const properties = encoded.properties as Record<string, unknown> | undefined;

      expect(properties?.cause).toBeUndefined();
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
