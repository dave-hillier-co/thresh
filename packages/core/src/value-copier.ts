import { GrainCallAbortedError, GrainTaskCanceledError } from "./errors";
import { CancellationTokenPlaceholder, GrainCancellationToken } from "./grain-cancellation-token";
import { GrainId } from "./grain-id";
import { keyToString } from "./grain-key";
import { grainReferenceIdentity, type GrainReferenceIdentity } from "./grain-reference";
import { Guid } from "./guid";
import { isImmutable } from "./immutable";
import { SiloAddress } from "./silo-address";
import {
  CircularReferenceError,
  decodeValue,
  encodeNested,
  hasSurrogates,
  surrogateFor,
  typedArrayConstructor,
  typedArrayKind,
  UnsupportedValueError,
  type CodecContext,
  type SurrogateDescriptor,
} from "./value-codec";

/**
 * A deep copier whose result is the value codec's round trip (`encodeValue`, a transport, then
 * `decodeValue`), built directly rather than through the encoded form: the analogue of Orleans'
 * `DeepCopier`, which a silo runs over the arguments and the response of a call that does not
 * leave it (`InsideRuntimeClient`, `CopyContext`) so that placement never changes what a callee
 * can see or mutate.
 *
 * The contract is equivalence with the codec, not a copy of the source: a value comes out exactly
 * as a callee on another silo would receive it. So a class instance with no surrogate is flattened
 * to a plain object of its own enumerable keys, a `Map` or array subclass arrives as the built-in,
 * an object member that is `undefined` is dropped while a positional one (an argument, an element,
 * a `Map` key or value, a `Set` member) is kept, the same object reached twice is copied twice, and
 * every value the codec refuses — a cycle, a sparse array, a function, a `RegExp` and the rest —
 * is refused here with the same error naming the same path. `value-copier.equivalence.test.ts`
 * holds it to that over both serializers.
 *
 * Each case below mirrors one branch of `encodeInner` and its `decodeValue` counterpart, in the
 * same order, so the first refusal found is the codec's first refusal. The rare branches that are
 * not worth mirroring by hand — an `Error` (rebuilt through the known-error table) and a
 * surrogate whose fields collide with the envelope's own keys — are handed to the codec itself.
 * A failure the codec would only meet while DECODING (a surrogate's `decode` throwing, a typed
 * array of a kind this build cannot build) is held until the walk ends and thrown then, so a
 * refusal the encode would have met later still wins, as it does in the round trip.
 *
 * One deliberate departure, and only one: a value marked with `immutable` is returned as itself,
 * uncopied, wherever it sits (Orleans' `Immutable<T>`); the codec ignores the mark.
 */

/**
 * The codec tags this module has a case for. Kept beside the cases rather than derived, so that
 * the equivalence test fails when the codec grows a tag nobody taught the copier.
 */
export const COPIED_TAGS: ReadonlySet<string> = new Set([
  "undefined", // copyValue / copyElement: a positional undefined stays undefined
  "negZero", // copyScalar
  "bigint", // copyScalar
  "bytes", // copyObject: Uint8Array (the JSON transport's tag for it)
  "typedArray", // copyTypedArray
  "date",
  "guid",
  "grainId",
  "cancellationToken",
  "domException",
  "callAborted",
  "taskCanceled",
  "silo",
  "grainRef",
  "map",
  "set",
  "error", // viaCodec
  "plainObject", // copyPlainObject's escaped branch
]);

const T = "$thresh";
const V = "$tsvv";
const DANGEROUS_KEYS = new Set(["__proto__", "constructor", "prototype"]);

interface Walk {
  readonly seen: Set<unknown>;
  readonly ctx: CodecContext;
  /** The first failure the round trip would meet only while decoding; thrown when the walk ends. */
  deferred: { readonly error: unknown } | undefined;
}

/**
 * Copy `value` so that it is what the codec round trip would deliver: no object shared with the
 * source, runtime value types rebuilt, grain references rehydrated through `ctx` as the
 * serializer would. Throws what `encodeValue` throws for a value it cannot represent.
 */
export function copyValue<T>(value: T, ctx: CodecContext = {}): T {
  const walk: Walk = { seen: new Set(), ctx, deferred: undefined };
  // A top-level undefined is tagged by `encodeValue` and decoded back to itself.
  const out = value === undefined ? undefined : copyInner(value, walk, "$");
  if (walk.deferred !== undefined) throw walk.deferred.error;
  return out as T;
}

/** A positional slot (`encodeElement`): `undefined` is a value here, kept as itself. */
function copyElement(value: unknown, walk: Walk, path: string): unknown {
  return value === undefined ? undefined : copyInner(value, walk, path);
}

function copyInner(value: unknown, walk: Walk, path: string): unknown {
  if (typeof value !== "object" || value === null) return copyScalar(value, walk, path);
  return copyObject(value, walk, path);
}

function copyScalar(value: unknown, walk: Walk, path: string): unknown {
  // `-0` and a bigint are tagged before any surrogate is asked, and decode to themselves.
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Object.is(value, -0)) return value;
  const surrogate = surrogateFor(value);
  if (surrogate !== undefined) return copySurrogate(value, surrogate, walk, path);
  if (typeof value === "function") throw new UnsupportedValueError(path, "a function");
  if (typeof value === "symbol") throw new UnsupportedValueError(path, "a symbol");
  return value;
}

function copyObject(value: object, walk: Walk, path: string): unknown {
  // The one case the codec does not have: a value its author marked as safe to share.
  if (isImmutable(value)) return value;
  const proto: unknown = Object.getPrototypeOf(value);
  // Fast paths. An object whose prototype is `Object.prototype` (or an array whose prototype is
  // `Array.prototype`) can match none of the `instanceof` branches the codec tries first, so only
  // the two checks that can still claim it — a grain reference, a surrogate — are asked.
  if (proto === Object.prototype && !ArrayBuffer.isView(value) && !Array.isArray(value)) {
    const claimed = copyRefOrSurrogate(value, walk, path);
    return claimed === UNCLAIMED ? copyPlainObject(value, walk, path) : claimed;
  }
  if (proto === Array.prototype && Array.isArray(value)) {
    const claimed = copyRefOrSurrogate(value, walk, path);
    return claimed === UNCLAIMED ? copyArray(value, walk, path) : claimed;
  }

  if (value instanceof Uint8Array) return new Uint8Array(value);
  if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
    return copyTypedArray(value, walk, path);
  }
  if (value instanceof Date) return new Date(value.getTime());
  if (value instanceof Guid) return Guid.parse(value.toString());
  if (value instanceof GrainId) return copyGrainId(value);
  if (value instanceof GrainCancellationToken) {
    return new CancellationTokenPlaceholder(
      value.tokenId,
      value.isCancellationRequested,
      value.asSignal === true,
    );
  }
  if (value instanceof CancellationTokenPlaceholder) {
    return new CancellationTokenPlaceholder(
      value.tokenId,
      value.cancelled,
      value.asSignal === true,
    );
  }
  if (value instanceof DOMException) return new DOMException(value.message, value.name);
  if (value instanceof GrainCallAbortedError) return new GrainCallAbortedError(value.message);
  if (value instanceof GrainTaskCanceledError) return new GrainTaskCanceledError(value.message);
  if (value instanceof SiloAddress) {
    return new SiloAddress(value.podName, value.podUid, value.endpoint);
  }
  const claimed = copyRefOrSurrogate(value, walk, path);
  if (claimed !== UNCLAIMED) return claimed;

  if (value instanceof RegExp) throw new UnsupportedValueError(path, "a RegExp");
  if (value instanceof Promise) throw new UnsupportedValueError(path, "a Promise");
  if (value instanceof WeakMap) throw new UnsupportedValueError(path, "a WeakMap");
  if (value instanceof WeakSet) throw new UnsupportedValueError(path, "a WeakSet");
  if (value instanceof ArrayBuffer) throw new UnsupportedValueError(path, "an ArrayBuffer");
  if (value instanceof DataView) throw new UnsupportedValueError(path, "a DataView");

  if (value instanceof Map) return copyMap(value, walk, path);
  if (value instanceof Set) return copySet(value, walk, path);
  if (Array.isArray(value)) return copyArray(value, walk, path);
  if (value instanceof Error) return viaCodec(value, walk, path);
  return copyPlainObject(value, walk, path);
}

/** Returned by `copyRefOrSurrogate` when neither claims the value (`undefined` is a real result). */
const UNCLAIMED: unique symbol = Symbol("unclaimed");

/** The two checks the codec makes ahead of its refusals and its structural branches. */
function copyRefOrSurrogate(value: object, walk: Walk, path: string): unknown {
  const ref = grainReferenceIdentity(value);
  if (ref !== undefined) return copyGrainReference(ref, walk.ctx);
  const surrogate = surrogateFor(value);
  if (surrogate !== undefined) return copySurrogate(value, surrogate, walk, path);
  return UNCLAIMED;
}

function copyGrainId(id: GrainId): GrainId {
  return GrainId.parse(`${id.type}/${keyToString(id.key)}`, id.keyKind);
}

function copyGrainReference(ref: GrainReferenceIdentity, ctx: CodecContext): unknown {
  const identity: GrainReferenceIdentity = {
    grainId: copyGrainId(ref.grainId),
    interfaceId: ref.interfaceId,
  };
  return ctx.resolveGrainReference ? ctx.resolveGrainReference(identity) : identity;
}

function copyTypedArray(view: ArrayBufferView, walk: Walk, path: string): unknown {
  const kind = typedArrayKind(view);
  const source = view as unknown as ArrayLike<number | bigint>;
  // With a surrogate registered, each element is offered to it, as `encodeElement` offers it;
  // with none, an element comes back as itself and the view can be copied whole.
  let values: (number | bigint)[] | undefined;
  if (hasSurrogates()) {
    values = [];
    for (let i = 0; i < source.length; i++) {
      values.push(copyElement(source[i], walk, `${path}[${i}]`) as number | bigint);
    }
  }
  const ctor = typedArrayConstructor(kind);
  if (ctor === undefined) {
    defer(walk, new Error(`decodeValue: unknown typed array kind "${kind}"`));
    return undefined;
  }
  return values === undefined
    ? new (ctor as unknown as new (from: ArrayLike<number | bigint>) => object)(source)
    : new ctor(values);
}

function copyMap(value: Map<unknown, unknown>, walk: Walk, path: string): Map<unknown, unknown> {
  if (walk.seen.has(value)) throw new CircularReferenceError(path);
  walk.seen.add(value);
  try {
    const out = new Map<unknown, unknown>();
    let i = 0;
    for (const [k, v] of value) {
      const key = copyElement(k, walk, `${path}[${i}].key`);
      out.set(key, copyElement(v, walk, `${path}[${i}].value`));
      i++;
    }
    return out;
  } finally {
    walk.seen.delete(value);
  }
}

function copySet(value: Set<unknown>, walk: Walk, path: string): Set<unknown> {
  if (walk.seen.has(value)) throw new CircularReferenceError(path);
  walk.seen.add(value);
  try {
    const out = new Set<unknown>();
    let i = 0;
    for (const v of value) out.add(copyElement(v, walk, `${path}[${i++}]`));
    return out;
  } finally {
    walk.seen.delete(value);
  }
}

function copyArray(value: readonly unknown[], walk: Walk, path: string): unknown[] {
  if (walk.seen.has(value)) throw new CircularReferenceError(path);
  const length = value.length;
  for (let i = 0; i < length; i++) {
    if (!(i in value))
      throw new UnsupportedValueError(path, `a sparse array (index ${i} is a hole)`);
  }
  walk.seen.add(value);
  try {
    const out: unknown[] = new Array(length);
    for (let i = 0; i < length; i++) out[i] = copyElement(value[i], walk, `${path}[${i}]`);
    return out;
  } finally {
    walk.seen.delete(value);
  }
}

function copyPlainObject(value: object, walk: Walk, path: string): Record<string, unknown> {
  if (walk.seen.has(value)) throw new CircularReferenceError(path);
  walk.seen.add(value);
  try {
    const source = value as Record<string, unknown>;
    const keys = Object.keys(value);
    const out: Record<string, unknown> = {};
    if (keys.some((k) => k === T || DANGEROUS_KEYS.has(k))) {
      // The codec's `plainObject` envelope: entries carried as positional pairs (so an undefined
      // member is kept) and rebuilt by definition, never by assignment.
      for (const k of keys) setOwn(out, k, copyElement(source[k], walk, `${path}.${k}`));
      return out;
    }
    for (const k of keys) {
      const v = source[k];
      // An undefined member travels as an omitted key — unless a surrogate claims `undefined`.
      if (v === undefined && surrogateFor(v) === undefined) continue;
      out[k] = copyInner(v, walk, `${path}.${k}`);
    }
    return out;
  } finally {
    walk.seen.delete(value);
  }
}

function copySurrogate(
  value: unknown,
  surrogate: SurrogateDescriptor,
  walk: Walk,
  path: string,
): unknown {
  if (walk.seen.has(value)) throw new CircularReferenceError(path);
  walk.seen.add(value);
  let fields: Record<string, unknown>;
  try {
    fields = surrogate.encode(value);
  } catch (error) {
    walk.seen.delete(value);
    throw error;
  }
  const keys = Object.keys(fields);
  if (keys.some((k) => k === T || k === V || DANGEROUS_KEYS.has(k))) {
    // A field named like the envelope's own keys, or a dangerous one, reshapes the envelope the
    // codec writes; let the codec say what that comes back as.
    walk.seen.delete(value);
    return viaCodec(value, walk, path);
  }
  try {
    const decoded: Record<string, unknown> = {};
    for (const k of keys) {
      const v = fields[k];
      if (v === undefined && surrogateFor(v) === undefined) continue;
      setOwn(decoded, k, copyInner(v, walk, `${path}.${k}`));
    }
    try {
      return surrogate.decode(decoded, walk.ctx);
    } catch (error) {
      defer(walk, error);
      return undefined;
    }
  } finally {
    walk.seen.delete(value);
  }
}

/**
 * The codec's own path, for the cases not mirrored by hand: encode the subtree against the walk's
 * cycle set and path (so a cycle or a refusal inside it reads exactly as in one whole encode),
 * carry it as a transport would, and decode it.
 */
function viaCodec(value: unknown, walk: Walk, path: string): unknown {
  const encoded = encodeNested(value, walk.seen, path);
  try {
    return decodeValue(asTransported(encoded), walk.ctx);
  } catch (error) {
    defer(walk, error);
    return undefined;
  }
}

/**
 * What a transport hands `decodeValue` for an encoded value: an object member that is
 * `undefined` omitted (MessagePack's `ignoreUndefined`, `JSON.stringify`'s own omission) and
 * binary a fresh buffer rather than the sender's.
 */
function asTransported(encoded: unknown): unknown {
  if (encoded instanceof Uint8Array) return new Uint8Array(encoded);
  // Not `encoded.map`: the codec's own `map` over an array subclass keeps the subclass (through
  // `Symbol.species`), which no transport does.
  if (Array.isArray(encoded)) return Array.from(encoded as unknown[], asTransported);
  if (encoded === null || typeof encoded !== "object") return encoded;
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(encoded)) {
    if (v !== undefined) setOwn(out, k, asTransported(v));
  }
  return out;
}

function defer(walk: Walk, error: unknown): void {
  walk.deferred ??= { error };
}

function setOwn(target: object, key: string, value: unknown): void {
  Object.defineProperty(target, key, {
    value,
    writable: true,
    enumerable: true,
    configurable: true,
  });
}
