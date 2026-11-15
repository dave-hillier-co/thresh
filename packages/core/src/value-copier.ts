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

/**
 * The containers currently being copied, innermost last: the codec's `seen` set, which is how it
 * tells a cycle (a container inside itself) from a value merely reached twice. Entries come and go
 * strictly last-in first-out, so a stack serves, scanned linearly while it is shallow — cheaper
 * than hashing every container of a wide, shallow graph — and backed by a set once it is deep.
 */
class Ancestors {
  private readonly stack: unknown[] = [];
  private indexed: Set<unknown> | undefined;

  has(value: unknown): boolean {
    if (this.indexed !== undefined) return this.indexed.has(value);
    for (let i = this.stack.length - 1; i >= 0; i--) if (this.stack[i] === value) return true;
    return false;
  }

  add(value: unknown): void {
    this.stack.push(value);
    if (this.indexed !== undefined) this.indexed.add(value);
    else if (this.stack.length > 32) this.indexed = new Set(this.stack);
  }

  delete(value: unknown): void {
    this.stack.pop();
    this.indexed?.delete(value);
  }

  /** A set of the same entries, for the codec's own walk (which adds and removes its own). */
  toSet(): Set<unknown> {
    return new Set(this.stack);
  }
}

interface Walk {
  readonly seen: Ancestors;
  readonly ctx: CodecContext;
  /** The first failure the round trip would meet only while decoding; thrown when the walk ends. */
  deferred: { readonly error: unknown } | undefined;
}

/**
 * Where a value sits, for the path a refusal names — built only when one is thrown. The codec
 * formats a path string for every node it visits; here a position is a (parent, kind, key) triple
 * passed down, and a container allocates one `Frame` for its children to point at.
 */
interface Frame {
  readonly parent: Frame | undefined;
  readonly kind: Kind;
  readonly key: string | number;
}

/** `$`, `[i]`, `[i].key`, `[i].value`, `.k`: the codec's path segments. */
type Kind = 0 | 1 | 2 | 3 | 4;
const ROOT: Kind = 0;
const INDEX: Kind = 1;
const MAP_KEY: Kind = 2;
const MAP_VALUE: Kind = 3;
const PROPERTY: Kind = 4;

function pathOf(parent: Frame | undefined, kind: Kind, key: string | number): string {
  if (kind === ROOT) return "$";
  const base = parent === undefined ? "$" : pathOf(parent.parent, parent.kind, parent.key);
  switch (kind) {
    case INDEX:
      return `${base}[${key}]`;
    case MAP_KEY:
      return `${base}[${key}].key`;
    case MAP_VALUE:
      return `${base}[${key}].value`;
    default:
      return `${base}.${key}`;
  }
}

/**
 * Copy `value` so that it is what the codec round trip would deliver: no object shared with the
 * source, runtime value types rebuilt, grain references rehydrated through `ctx` as the
 * serializer would. Throws what `encodeValue` throws for a value it cannot represent.
 */
export function copyValue<T>(value: T, ctx: CodecContext = {}): T {
  const walk: Walk = { seen: new Ancestors(), ctx, deferred: undefined };
  // A top-level undefined is tagged by `encodeValue` and decoded back to itself.
  const out = value === undefined ? undefined : copyInner(value, walk, undefined, ROOT, "$");
  if (walk.deferred !== undefined) throw walk.deferred.error;
  return out as T;
}

/** A positional slot (`encodeElement`): `undefined` is a value here, kept as itself. */
function copyElement(
  value: unknown,
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): unknown {
  return value === undefined ? undefined : copyInner(value, walk, parent, kind, key);
}

function copyInner(
  value: unknown,
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): unknown {
  if (typeof value === "object" && value !== null) {
    return copyObject(value, walk, parent, kind, key);
  }
  // `-0` and a bigint are tagged before any surrogate is asked, and decode to themselves.
  if (typeof value === "bigint") return value;
  if (typeof value === "number" && Object.is(value, -0)) return value;
  if (hasSurrogates()) {
    const surrogate = surrogateFor(value);
    if (surrogate !== undefined) return copySurrogate(value, surrogate, walk, parent, kind, key);
  }
  if (typeof value === "function") {
    throw new UnsupportedValueError(pathOf(parent, kind, key), "a function");
  }
  if (typeof value === "symbol") {
    throw new UnsupportedValueError(pathOf(parent, kind, key), "a symbol");
  }
  return value;
}

function copyObject(
  value: object,
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): unknown {
  // The one case the codec does not have: a value its author marked as safe to share.
  if (isImmutable(value)) return value;
  const proto: unknown = Object.getPrototypeOf(value);
  // Fast paths. An object whose prototype is `Object.prototype` (or an array whose prototype is
  // `Array.prototype`) can match none of the `instanceof` branches the codec tries first, so only
  // the two checks that can still claim it — a grain reference, a surrogate — are asked.
  if (proto === Array.prototype && Array.isArray(value)) {
    const claimed = copyRefOrSurrogate(value, walk, parent, kind, key);
    return claimed === UNCLAIMED ? copyArray(value, walk, parent, kind, key) : claimed;
  }
  if (proto === Object.prototype && !ArrayBuffer.isView(value) && !Array.isArray(value)) {
    const claimed = copyRefOrSurrogate(value, walk, parent, kind, key);
    return claimed === UNCLAIMED ? copyPlainObject(value, walk, parent, kind, key) : claimed;
  }

  if (value instanceof Uint8Array) return new Uint8Array(value);
  if (ArrayBuffer.isView(value) && !(value instanceof DataView)) {
    return copyTypedArray(value, walk, parent, kind, key);
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
  const claimed = copyRefOrSurrogate(value, walk, parent, kind, key);
  if (claimed !== UNCLAIMED) return claimed;

  const refusal = unsupported(value);
  if (refusal !== undefined) throw new UnsupportedValueError(pathOf(parent, kind, key), refusal);

  if (value instanceof Map) return copyMap(value, walk, parent, kind, key);
  if (value instanceof Set) return copySet(value, walk, parent, kind, key);
  if (Array.isArray(value)) return copyArray(value, walk, parent, kind, key);
  if (value instanceof Error) return viaCodec(value, walk, pathOf(parent, kind, key));
  return copyPlainObject(value, walk, parent, kind, key);
}

/** The built-ins the codec refuses (after asking the surrogates), in its order, and its words. */
function unsupported(value: object): string | undefined {
  if (value instanceof RegExp) return "a RegExp";
  if (value instanceof Promise) return "a Promise";
  if (value instanceof WeakMap) return "a WeakMap";
  if (value instanceof WeakSet) return "a WeakSet";
  if (value instanceof ArrayBuffer) return "an ArrayBuffer";
  if (value instanceof DataView) return "a DataView";
  return undefined;
}

/** Returned by `copyRefOrSurrogate` when neither claims the value (`undefined` is a real result). */
const UNCLAIMED: unique symbol = Symbol("unclaimed");

/** The two checks the codec makes ahead of its refusals and its structural branches. */
function copyRefOrSurrogate(
  value: object,
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): unknown {
  const ref = grainReferenceIdentity(value);
  if (ref !== undefined) return copyGrainReference(ref, walk.ctx);
  if (!hasSurrogates()) return UNCLAIMED;
  const surrogate = surrogateFor(value);
  if (surrogate !== undefined) return copySurrogate(value, surrogate, walk, parent, kind, key);
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

function copyTypedArray(
  view: ArrayBufferView,
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): unknown {
  const arrayKind = typedArrayKind(view);
  const source = view as unknown as ArrayLike<number | bigint>;
  // With a surrogate registered, each element is offered to it, as `encodeElement` offers it;
  // with none, an element comes back as itself and the view can be copied whole.
  let values: (number | bigint)[] | undefined;
  if (hasSurrogates()) {
    const here: Frame = { parent, kind, key };
    values = [];
    for (let i = 0; i < source.length; i++) {
      values.push(copyElement(source[i], walk, here, INDEX, i) as number | bigint);
    }
  }
  const ctor = typedArrayConstructor(arrayKind);
  if (ctor === undefined) {
    defer(walk, new Error(`decodeValue: unknown typed array kind "${arrayKind}"`));
    return undefined;
  }
  return values === undefined
    ? new (ctor as unknown as new (from: ArrayLike<number | bigint>) => object)(source)
    : new ctor(values);
}

function copyMap(
  value: Map<unknown, unknown>,
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): Map<unknown, unknown> {
  if (walk.seen.has(value)) throw new CircularReferenceError(pathOf(parent, kind, key));
  walk.seen.add(value);
  try {
    const here: Frame = { parent, kind, key };
    const out = new Map<unknown, unknown>();
    let i = 0;
    for (const [k, v] of value) {
      const copiedKey = copyElement(k, walk, here, MAP_KEY, i);
      out.set(copiedKey, copyElement(v, walk, here, MAP_VALUE, i));
      i++;
    }
    return out;
  } finally {
    walk.seen.delete(value);
  }
}

function copySet(
  value: Set<unknown>,
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): Set<unknown> {
  if (walk.seen.has(value)) throw new CircularReferenceError(pathOf(parent, kind, key));
  walk.seen.add(value);
  try {
    const here: Frame = { parent, kind, key };
    const out = new Set<unknown>();
    let i = 0;
    for (const v of value) out.add(copyElement(v, walk, here, INDEX, i++));
    return out;
  } finally {
    walk.seen.delete(value);
  }
}

function copyArray(
  value: readonly unknown[],
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): unknown[] {
  if (walk.seen.has(value)) throw new CircularReferenceError(pathOf(parent, kind, key));
  const length = value.length;
  for (let i = 0; i < length; i++) {
    if (!(i in value)) {
      throw new UnsupportedValueError(
        pathOf(parent, kind, key),
        `a sparse array (index ${i} is a hole)`,
      );
    }
  }
  walk.seen.add(value);
  try {
    const here: Frame = { parent, kind, key };
    const out: unknown[] = new Array(length);
    for (let i = 0; i < length; i++) out[i] = copyElement(value[i], walk, here, INDEX, i);
    return out;
  } finally {
    walk.seen.delete(value);
  }
}

function copyPlainObject(
  value: object,
  walk: Walk,
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): Record<string, unknown> {
  if (walk.seen.has(value)) throw new CircularReferenceError(pathOf(parent, kind, key));
  walk.seen.add(value);
  try {
    const here: Frame = { parent, kind, key };
    const source = value as Record<string, unknown>;
    const keys = Object.keys(value);
    const out: Record<string, unknown> = {};
    if (keys.some((k) => k === T || DANGEROUS_KEYS.has(k))) {
      // The codec's `plainObject` envelope: entries carried as positional pairs (so an undefined
      // member is kept) and rebuilt by definition, never by assignment.
      for (const k of keys) setOwn(out, k, copyElement(source[k], walk, here, PROPERTY, k));
      return out;
    }
    for (const k of keys) {
      const v = source[k];
      // An undefined member travels as an omitted key — unless a surrogate claims `undefined`.
      if (v === undefined && surrogateFor(v) === undefined) continue;
      out[k] = copyInner(v, walk, here, PROPERTY, k);
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
  parent: Frame | undefined,
  kind: Kind,
  key: string | number,
): unknown {
  if (walk.seen.has(value)) throw new CircularReferenceError(pathOf(parent, kind, key));
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
    return viaCodec(value, walk, pathOf(parent, kind, key));
  }
  try {
    const here: Frame = { parent, kind, key };
    const decoded: Record<string, unknown> = {};
    for (const k of keys) {
      const v = fields[k];
      if (v === undefined && surrogateFor(v) === undefined) continue;
      setOwn(decoded, k, copyInner(v, walk, here, PROPERTY, k));
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
  const encoded = encodeNested(value, walk.seen.toSet(), path);
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
