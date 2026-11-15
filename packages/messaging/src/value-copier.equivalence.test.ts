import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  GrainCallAbortedError,
  GrainTaskCanceledError,
  InconsistentStateError,
  LimitExceededException,
  RejectionError,
} from "@thresh/core/errors";
import {
  CancellationTokenPlaceholder,
  GrainCancellationToken,
} from "@thresh/core/grain-cancellation-token";
import { GrainId } from "@thresh/core/grain-id";
import { GRAIN_REF, type GrainReferenceIdentity } from "@thresh/core/grain-reference";
import { Guid } from "@thresh/core/guid";
import { SiloAddress } from "@thresh/core/silo-address";
import {
  BUILT_IN_TAGS,
  encodeValue,
  registerSurrogate,
  unregisterSurrogate,
  type CodecContext,
  type SurrogateDescriptor,
} from "@thresh/core/value-codec";
import { COPIED_TAGS, copyValue } from "@thresh/core/value-copier";
import { JsonSerializer } from "@thresh/messaging/json-serializer";
import { MessagePackSerializer } from "@thresh/messaging/msgpack-serializer";
import type { Serializer } from "@thresh/messaging/serializer";

/**
 * The copier's contract: copying a value gives what the codec round trip gives, and refuses what
 * it refuses, with the same error. The round trip is taken through both serializers a silo can
 * use, so the contract holds for whichever one a cluster is configured with.
 */

class Money {
  constructor(
    readonly cents: number,
    readonly currency: string,
  ) {}
  format(): string {
    return `${this.cents} ${this.currency}`;
  }
}

/** A class with no surrogate: the codec flattens it to its own enumerable data. */
class Point {
  readonly label = "p";
  hidden!: number;
  constructor(
    readonly x: number,
    readonly y: number,
  ) {
    Object.defineProperty(this, "hidden", { value: 1, enumerable: false });
  }
  get length(): number {
    return Math.hypot(this.x, this.y);
  }
}

class Ledger {
  constructor(
    readonly entries: Map<string, Date>,
    readonly note: string | undefined,
  ) {}
}

class Samples extends Float64Array {}
class Bag extends Map<string, number> {}
class Items extends Array<number> {}

const moneySurrogate = {
  tag: "test.copier.money",
  test: (v: unknown) => v instanceof Money,
  encode: (m: Money) => ({ cents: m.cents, currency: m.currency }),
  decode: (f: Record<string, unknown>) => new Money(f.cents as number, f.currency as string),
};

const ledgerSurrogate = {
  tag: "test.copier.ledger",
  test: (v: unknown) => v instanceof Ledger,
  encode: (l: Ledger) => ({ entries: l.entries, note: l.note, extra: [undefined, -0] }),
  decode: (f: Record<string, unknown>) =>
    Object.assign(new Ledger(f.entries as Map<string, Date>, f.note as string | undefined), {
      keys: Object.keys(f),
      extra: f.extra,
    }),
};

/** A surrogate that cannot rebuild what it wrote: the round trip fails only while decoding. */
class Fragile {
  readonly n = 1;
}
const fragileSurrogate = {
  tag: "test.copier.fragile",
  test: (v: unknown) => v instanceof Fragile,
  encode: (f: Fragile) => ({ n: f.n }),
  decode: (): Fragile => {
    throw new RangeError("cannot rebuild a Fragile");
  },
};

/** A surrogate whose fields collide with the envelope's own keys. */
class Clash {
  readonly v = 2;
}
const clashSurrogate = {
  tag: "test.copier.clash",
  test: (v: unknown) => v instanceof Clash,
  encode: (c: Clash) => ({ $thresh: "x", $tsvv: 1, v: c.v, constructor: "c", prototype: [1] }),
  decode: (f: Record<string, unknown>) => ({ rebuilt: Object.keys(f) }),
};

/** A surrogate whose field NAME is chosen by the value. */
class Dynamic {
  constructor(readonly field: string) {}
}
const dynamicSurrogate = {
  tag: "test.copier.dynamic",
  test: (v: unknown) => v instanceof Dynamic,
  encode: (d: Dynamic) => ({ [d.field]: 1 }),
  decode: (f: Record<string, unknown>) => ({ rebuilt: Object.keys(f) }),
};

const surrogatesUnderTest = [
  moneySurrogate,
  ledgerSurrogate,
  fragileSurrogate,
  clashSurrogate,
  dynamicSurrogate,
];

beforeAll(() => {
  for (const s of surrogatesUnderTest) registerSurrogate(s as SurrogateDescriptor);
});
afterAll(() => {
  for (const s of surrogatesUnderTest) unregisterSurrogate(s.tag);
});

interface Link {
  depth: number;
  items: unknown[];
  next?: Link;
}

/** Nested objects (and an array at each level) `depth` levels deep. */
function chain(depth: number): Link {
  const head: Link = { depth: 0, items: [0] };
  let at = head;
  for (let i = 1; i < depth; i++) {
    const next: Link = { depth: i, items: [i] };
    at.next = next;
    at = next;
  }
  return head;
}

function grainRef(id: GrainId, interfaceId: number): object {
  return { [GRAIN_REF]: { grainId: id, interfaceId } satisfies GrainReferenceIdentity };
}

function token(cancelled: boolean, asSignal: boolean): GrainCancellationToken {
  const controller = new AbortController();
  if (cancelled) controller.abort();
  return new GrainCancellationToken({ tokenId: "tok-1", signal: controller.signal, asSignal });
}

const hostile = (): unknown =>
  JSON.parse('{"__proto__": {"polluted": true}, "constructor": 1, "prototype": 2, "ok": 3}');

function withProperties(): Error {
  const error = new RangeError("out of range");
  Object.assign(error, { code: 7, detail: { at: new Date(3), tags: new Set(["a"]) } });
  return error;
}

interface Sample {
  readonly make: () => unknown;
}

const corpus: Record<string, Sample> = {
  // Primitives.
  "a string": { make: () => "hello" },
  "an empty string": { make: () => "" },
  "a string with a surrogate pair": { make: () => "a\uD83D\uDE00b" },
  "a long string with a surrogate pair": { make: () => "x".repeat(300) + "\uD83D\uDE00" },
  "an integer": { make: () => 42 },
  "a float": { make: () => 1.5 },
  "a large integer": { make: () => 2 ** 60 },
  zero: { make: () => 0 },
  "negative zero": { make: () => -0 },
  NaN: { make: () => Number.NaN },
  Infinity: { make: () => Infinity },
  "negative Infinity": { make: () => -Infinity },
  true: { make: () => true },
  false: { make: () => false },
  null: { make: () => null },
  undefined: { make: () => undefined },
  "a bigint": { make: () => 123456789012345678901234567890n },
  "a negative bigint": { make: () => -5n },

  // Plain objects.
  "an empty object": { make: () => ({}) },
  "a nested object": { make: () => ({ a: 1, b: { c: "x", d: [1, { e: null }] } }) },
  "an object with integer-like keys": { make: () => ({ b: 1, 2: "two", a: 3, 1: "one" }) },
  "an object with an undefined member": { make: () => ({ a: 1, gone: undefined }) },
  "an object owning the tag key": { make: () => ({ $thresh: "date", value: 5 }) },
  "an object owning the tag key and an undefined member": {
    make: () => ({ $thresh: "x", gone: undefined }),
  },
  "an object owning the version key": { make: () => ({ $tsvv: 2, value: 1 }) },
  "an object shaped like the escape envelope": {
    make: () => ({ $thresh: "plainObject", entries: [["a", 1]] }),
  },
  "an object with the dangerous keys as own data": { make: hostile },
  "an object with a null prototype": {
    make: () => Object.assign(Object.create(null) as object, { a: 1 }),
  },
  "an object with a getter": {
    make: () => ({
      get computed(): number {
        return 9;
      },
    }),
  },
  "an object with a non-enumerable and a symbol key": {
    make: () => {
      const o: Record<string | symbol, unknown> = { shown: 1, [Symbol("s")]: 2 };
      Object.defineProperty(o, "hidden", { value: 3, enumerable: false });
      return o;
    },
  },
  "a boxed string": { make: () => new String("ab") },
  "a WeakRef": { make: () => new WeakRef({}) },

  // Arrays.
  "an empty array": { make: () => [] },
  "an array of mixed values": { make: () => [1, "two", null, true, [3], { four: 4 }] },
  "an array with undefined elements": { make: () => [undefined, 1, undefined] },
  "an array with extra own properties": {
    make: () => Object.assign([1, 2], { extra: "dropped" }),
  },
  "an array subclass": { make: () => Items.from([1, 2, 3]) },
  "the same object twice (a DAG)": {
    make: () => {
      const shared = { s: 1 };
      return { left: shared, right: shared, list: [shared, shared] };
    },
  },

  // Map and Set.
  "a Map with non-string keys": {
    make: () =>
      new Map<unknown, unknown>([
        ["a", 1],
        [2, "b"],
        [new Date(0), "epoch"],
        [{ k: 1 }, [1]],
      ]),
  },
  "a Map with undefined keys and values": {
    make: () =>
      new Map<unknown, unknown>([
        [undefined, 1],
        ["v", undefined],
      ]),
  },
  "a Map subclass": { make: () => new Bag([["a", 1]]) },
  "a Set": { make: () => new Set([1, "two", new Date(0), undefined, { o: 1 }]) },
  "an empty Set and Map": { make: () => [new Set(), new Map()] },

  // Date.
  "a Date": { make: () => new Date("2026-07-24T12:00:00.000Z") },
  "an invalid Date": { make: () => new Date(Number.NaN) },
  "a Date subclass": { make: () => new (class Stamp extends Date {})(5) },

  // Binary and typed arrays.
  "a Uint8Array": { make: () => new Uint8Array([0, 1, 2, 255]) },
  "an empty Uint8Array": { make: () => new Uint8Array() },
  "a Uint8Array view into a larger buffer": {
    make: () => new Uint8Array(new Uint8Array([9, 8, 7, 6, 5]).buffer, 1, 3),
  },
  "a Uint8Array subclass": { make: () => new (class Bytes extends Uint8Array {})([1, 2]) },
  Int8Array: { make: () => new Int8Array([-1, 0, 1, 127, -128]) },
  Uint8ClampedArray: { make: () => new Uint8ClampedArray([0, 128, 255]) },
  Int16Array: { make: () => new Int16Array([-32768, 0, 32767]) },
  Uint16Array: { make: () => new Uint16Array([0, 65535]) },
  Int32Array: { make: () => new Int32Array([-2147483648, 0, 2147483647]) },
  Uint32Array: { make: () => new Uint32Array([0, 4294967295]) },
  Float32Array: { make: () => new Float32Array([1.5, -1.5, 0, -0]) },
  Float64Array: { make: () => new Float64Array([1.5, -1.5, -0]) },
  "a Float64Array with NaN and Infinity": {
    make: () => new Float64Array([Number.NaN, Infinity, -Infinity]),
  },
  "a Float32Array with NaN and Infinity": {
    make: () => new Float32Array([Number.NaN, -Infinity]),
  },
  BigInt64Array: { make: () => new BigInt64Array([-1n, 0n, 1n]) },
  BigUint64Array: { make: () => new BigUint64Array([0n, 18446744073709551615n]) },
  "a typed array subclass": { make: () => Samples.from([1, 2, 3]) },

  // Built-in runtime value types.
  "a Guid": { make: () => Guid.parse("3F2504E0-4F89-11D3-9A0C-0305E82C3301") },
  "a string-keyed GrainId": { make: () => new GrainId("Account", "alice") },
  "an integer-keyed GrainId": { make: () => GrainId.parse("Counter/42", "integer") },
  "a Guid-keyed GrainId": {
    make: () => new GrainId("Session", Guid.parse("3f2504e0-4f89-11d3-9a0c-0305e82c3301")),
  },
  "a SiloAddress": { make: () => new SiloAddress("pod-0", "uid-0", "10.0.0.1:11111") },
  "a grain reference": { make: () => grainRef(new GrainId("Account", "bob"), 17) },
  "a cancellation token": { make: () => token(false, false) },
  "a cancelled cancellation token standing for a signal": { make: () => token(true, true) },
  "a cancellation token placeholder": {
    make: () => new CancellationTokenPlaceholder("tok-2", true, true),
  },

  // Errors carried as values, and the cancellation family.
  "a DOMException": { make: () => new DOMException("stopped", "AbortError") },
  "a GrainCallAbortedError": { make: () => new GrainCallAbortedError("aborted") },
  "a GrainTaskCanceledError": { make: () => new GrainTaskCanceledError("canceled") },
  "a plain Error": { make: () => new Error("boom") },
  "a built-in error with properties": { make: withProperties },
  "a Thresh error": { make: () => new LimitExceededException("calls", 11, 10) },
  "a Thresh error with fields": {
    make: () => new InconsistentStateError("etag mismatch", "e1", "e2"),
  },
  "a RejectionError": { make: () => new RejectionError("gone", "siloUnavailable") },
  "an unregistered error class": {
    make: () => Object.assign(new (class Overdrawn extends Error {})("overdrawn"), { by: 5 }),
  },
  "an error with a cause chain": {
    make: () => new Error("outer", { cause: new TypeError("inner", { cause: { why: 1 } }) }),
  },
  "an error with an undefined cause": { make: () => new Error("x", { cause: undefined }) },
  "an AggregateError": {
    make: () => new AggregateError([new Error("a"), new RangeError("b")], "many"),
  },

  // Surrogates.
  "a registered surrogate": { make: () => new Money(500, "GBP") },
  "a surrogate whose fields collide with the envelope's keys": { make: () => new Clash() },
  "a surrogate with nested fields and an undefined field": {
    make: () => new Ledger(new Map([["opened", new Date(7)]]), undefined),
  },

  // Class instances with no surrogate.
  "a class instance": { make: () => new Point(3, 4) },
  "a class instance holding a surrogate and a Date": {
    make: () => Object.assign(new Point(1, 2), { price: new Money(1, "EUR"), at: new Date(1) }),
  },

  // Depth: past the copier's switch from a scanned stack of ancestors to an indexed one.
  "a chain 60 levels deep": { make: () => chain(60) },

  // A mixture, as a call's argument list.
  "an argument list": {
    make: () => [
      "id-1",
      undefined,
      { when: new Date(9), tags: new Set(["x"]), scores: new Map([["a", [1, 2]]]) },
      new Uint8Array([1, 2, 3]),
      [10n, -0, Guid.parse("00000000-0000-0000-0000-000000000001")],
    ],
  },
};

/** Positions a value can occupy, each with its own rules for `undefined` and keys. */
const positions: Record<string, (v: unknown) => unknown> = {
  "on its own": (v) => v,
  "as an array element": (v) => [v, 1],
  "as an object member": (v) => ({ m: v, after: 2 }),
  "as a member of an escaped object": (v) => ({ $thresh: "x", m: v }),
  "as a Map key and value": (v) => new Map<unknown, unknown>([[v, v]]),
  "as a Set member": (v) => new Set([v]),
  "in a surrogate's field": (v) => new Ledger(new Map([["k", new Date(1)]]), v as string),
  "in a class instance": (v) => Object.assign(new Point(0, 0), { v }),
  "in an error's properties": (v) => Object.assign(new Error("carrier"), { data: v }),
};

/**
 * A structural description that two values share exactly when they are deep-equal with the same
 * prototypes, the same own keys (in order, with their enumerability) and the same leaf values by
 * `Object.is`. A freshly built cancellation error's stack names wherever it was built, so its
 * stack is left out; every other error's stack is the sender's, carried, and is compared.
 */
function shape(value: unknown, depth = 0): unknown {
  if (depth > 400) return "<too deep>";
  if (value === null) return ["null"];
  switch (typeof value) {
    case "number":
      return ["number", Object.is(value, -0) ? "-0" : String(value)];
    case "bigint":
      return ["bigint", value.toString()];
    case "string":
      return ["string", value];
    case "boolean":
      return ["boolean", value];
    case "undefined":
      return ["undefined"];
    case "function":
    case "symbol":
      return [typeof value];
  }
  const object = value as object;
  const proto: unknown = Object.getPrototypeOf(object);
  const protoName =
    proto === null
      ? "null"
      : ((proto as { constructor?: { name?: string } }).constructor?.name ?? "anonymous");
  const out: unknown[] = [protoName];
  if (object instanceof Date) out.push(String(object.getTime()));
  if (object instanceof Map) {
    out.push([...object].map(([k, v]) => [shape(k, depth + 1), shape(v, depth + 1)]));
  }
  if (object instanceof Set) out.push([...object].map((v) => shape(v, depth + 1)));
  if (ArrayBuffer.isView(object) && !(object instanceof DataView)) {
    out.push(
      Array.from(object as unknown as ArrayLike<unknown>, (v) => shape(v, depth + 1)),
      (object as unknown as { length: number }).length,
    );
  }
  if (object instanceof DOMException) out.push(object.name, object.message);
  const freshlyBuilt =
    object instanceof GrainCallAbortedError ||
    object instanceof GrainTaskCanceledError ||
    object instanceof DOMException;
  const keys = Reflect.ownKeys(object).filter((k) => !(freshlyBuilt && k === "stack"));
  out.push(
    keys.map((k) => {
      const d = Object.getOwnPropertyDescriptor(object, k)!;
      const v = "value" in d ? d.value : "<accessor>";
      return [
        String(k),
        d.enumerable,
        ArrayBuffer.isView(object) ? "<element>" : shape(v, depth + 1),
      ];
    }),
  );
  return out;
}

type Outcome = { ok: unknown } | { error: { type: string; message: string; path?: unknown } };

function outcome(run: () => unknown): Outcome {
  try {
    return { ok: shape(run()) };
  } catch (error) {
    return {
      error: {
        type: (error as Error).constructor.name,
        message: (error as Error).message,
        path: (error as { path?: unknown }).path,
      },
    };
  }
}

const resolving: CodecContext = {
  resolveGrainReference: (identity) => ({
    resolved: identity.grainId.toString(),
    kind: identity.grainId.keyKind,
    interfaceId: identity.interfaceId,
  }),
};

const transports: Array<[string, (ctx: CodecContext) => Serializer]> = [
  ["MessagePack", (ctx) => new MessagePackSerializer(ctx)],
  ["JSON", (ctx) => new JsonSerializer(ctx)],
];

describe.each(transports)("copyValue matches the %s round trip", (_name, make) => {
  describe.each(Object.entries(positions))("%s", (_position, place) => {
    it.each(Object.entries(corpus))("%s", (_label, sample) => {
      for (const ctx of [resolving, {}]) {
        const serializer = make(ctx);
        const viaWire = outcome(() =>
          serializer.deserialize(serializer.serialize(place(sample.make()))),
        );
        const viaCopy = outcome(() => copyValue(place(sample.make()), ctx));
        expect(viaCopy).toEqual(viaWire);
      }
    });
  });
});

function cyclic(): Record<string, unknown> {
  const o: Record<string, unknown> = { a: 1 };
  o.self = o;
  return o;
}

const refused: Record<string, () => unknown> = {
  "a string with a lone high surrogate": () => "a\uD800b",
  "a string with a lone low surrogate": () => "a\uDC00b",
  "a long string with a lone surrogate": () => "x".repeat(300) + "\uD800",
  "an object key with a lone surrogate": () => ({ ["k\uD800"]: 1 }),
  "an escaped object key with a lone surrogate": () => ({ $thresh: "x", ["k\uDC00"]: 1 }),
  "a lone surrogate in a Map key": () => new Map([["\uD800", 1]]),
  "a lone surrogate in a surrogate's field": () => new Money(1, "\uD800"),
  "a lone surrogate in a surrogate's field name": () => new Dynamic("f\uD800"),
  "a lone surrogate in an error's properties": () =>
    Object.assign(new Error("e"), { text: "\uD800" }),
  "a RegExp": () => /x/g,
  "a Promise": () => Promise.resolve(1),
  "a WeakMap": () => new WeakMap(),
  "a WeakSet": () => new WeakSet(),
  "an ArrayBuffer": () => new ArrayBuffer(4),
  "a DataView": () => new DataView(new ArrayBuffer(4)),
  "a function": () => () => 1,
  "a symbol": () => Symbol("s"),
  "a sparse array": () => [1, , 3], // eslint-disable-line no-sparse-arrays
  "a deeply nested function": () => ({ a: [{ b: new Map([["k", { c: () => 1 }]]) }] }),
  "a function in a Set": () => new Set([1, () => 1]),
  "a self-referencing object": cyclic,
  "a self-referencing array": () => {
    const a: unknown[] = [1];
    a.push(a);
    return a;
  },
  "a cycle through a Map": () => {
    const m = new Map<string, unknown>();
    m.set("me", { m });
    return m;
  },
  "a cycle through a Set": () => {
    const s = new Set<unknown>();
    s.add([s]);
    return s;
  },
  "a cycle through a class instance": () => {
    const p = new Point(1, 1) as Point & { me?: unknown };
    p.me = { p };
    return p;
  },
  "a cycle through an error's cause": () => {
    const e = new Error("loop");
    e.cause = e;
    return e;
  },
  "a cycle from an error back to its holder": () => {
    const holder: Record<string, unknown> = {};
    holder.err = Object.assign(new Error("e"), { back: holder });
    return holder;
  },
  "a cycle through a surrogate's fields": () => {
    const entries = new Map<string, Date>();
    const ledger = new Ledger(entries, "n");
    (entries as Map<string, unknown>).set("me", ledger);
    return ledger;
  },
  "a refused value after a good one": () => [new Date(1), { ok: 1 }, /re/],
  "a cycle 60 levels deep": () => {
    const head = chain(60);
    let tail = head;
    while (tail.next !== undefined) tail = tail.next;
    tail.next = { back: [head] } as unknown as Link;
    return head;
  },
  "a cycle closing 60 levels down onto the 50th": () => {
    const head = chain(60);
    let tail = head;
    let fiftieth = head;
    for (let i = 0; tail.next !== undefined; i++) {
      if (i === 50) fiftieth = tail;
      tail = tail.next;
    }
    tail.next = fiftieth;
    return head;
  },
  "a function in an error's properties": () => Object.assign(new Error("e"), { fn: () => 1 }),
  "a surrogate that fails to decode": () => [new Fragile()],
  "a surrogate that fails to decode, then a function": () => [new Fragile(), () => 1],
  "two decode failures (the first wins)": () => [
    new Fragile(),
    Object.assign(new Error("e"), { f: new Fragile() }),
  ],
};

describe.each(transports)("copyValue refuses what the %s round trip refuses", (_name, make) => {
  describe.each(Object.entries(positions))("%s", (_position, place) => {
    it.each(Object.entries(refused))("%s", (_label, build) => {
      const serializer = make({});
      const viaWire = outcome(() => serializer.deserialize(serializer.serialize(place(build()))));
      const viaCopy = outcome(() => copyValue(place(build())));
      expect("error" in viaWire).toBe(true);
      expect(viaCopy).toEqual(viaWire);
    });
  });
});

/** Every object reachable from `value` through own properties, `Map` entries and `Set` members. */
function objectsIn(value: unknown, into = new Set<object>()): Set<object> {
  if (value === null || (typeof value !== "object" && typeof value !== "function")) return into;
  if (into.has(value)) return into;
  into.add(value);
  if (value instanceof Map) {
    for (const [k, v] of value) {
      objectsIn(k, into);
      objectsIn(v, into);
    }
  }
  if (value instanceof Set) for (const v of value) objectsIn(v, into);
  if (ArrayBuffer.isView(value)) {
    into.add(value.buffer);
    return into;
  }
  for (const key of Reflect.ownKeys(value)) {
    const d = Object.getOwnPropertyDescriptor(value, key);
    if (d !== undefined && "value" in d) objectsIn(d.value, into);
  }
  return into;
}

describe("copyValue shares no object with its source", () => {
  it.each(Object.entries(corpus))("%s", (_label, sample) => {
    const source = sample.make();
    const copy = copyValue(source, resolving);
    const before = objectsIn(source);
    expect([...objectsIn(copy)].filter((o) => before.has(o))).toEqual([]);
  });
});

describe("the copier's coverage of the codec", () => {
  function tagsIn(encoded: unknown, into: Set<string>): Set<string> {
    if (Array.isArray(encoded)) for (const v of encoded) tagsIn(v, into);
    else if (encoded !== null && typeof encoded === "object" && !ArrayBuffer.isView(encoded)) {
      const tag = (encoded as Record<string, unknown>).$thresh;
      if (typeof tag === "string") into.add(tag);
      for (const v of Object.values(encoded)) tagsIn(v, into);
    }
    return into;
  }

  it("has a case for every built-in tag the codec declares", () => {
    expect([...COPIED_TAGS].sort()).toEqual([...BUILT_IN_TAGS].sort());
  });

  it("is exercised against every built-in tag by the equivalence corpus", () => {
    const seen = new Set<string>();
    for (const sample of Object.values(corpus)) {
      tagsIn(encodeValue(sample.make()), seen);
      tagsIn(encodeValue(sample.make(), { binaryAsBase64: true }), seen);
    }
    expect([...BUILT_IN_TAGS].filter((tag) => !seen.has(tag))).toEqual([]);
  });
});
