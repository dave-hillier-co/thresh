import { CancellationTokenPlaceholder, GrainCancellationToken } from "./grain-cancellation-token";
import { GrainId } from "./grain-id";
import { keyToString, type GrainKeyKind } from "./grain-key";
import { grainReferenceIdentity, type GrainReferenceIdentity } from "./grain-reference";
import { Guid } from "./guid";
import { SiloAddress } from "./silo-address";

// Tag key for the plain, transport-safe form of a runtime value type. The JSON
// and MessagePack serializers and the durable providers share this
// transformation so what they can represent stays identical.
const T = "$thresh";
// Schema-version key, carried alongside `T` on every tagged envelope. Bumping
// it (per tag, if a tag's wire shape ever needs to change) lets a decoder
// distinguish old and new shapes during a rolling upgrade. Payloads produced
// before this field existed carry no `V` at all — `versionOf` below treats
// that absence as version 1, so old payloads keep decoding unchanged.
const V = "$tsvv";
const CURRENT_VERSION = 1;

export interface CodecContext {
  /** Rehydrate a grain reference identity into a working proxy on receive. */
  resolveGrainReference?: (identity: GrainReferenceIdentity) => unknown;
}

/**
 * Thrown by `encodeValue` when a value graph contains a cycle (an object that
 * transitively contains itself). The codec walks plain objects, arrays,
 * `Map`s and `Set`s eagerly with no visited-node output cache, so a cycle
 * would otherwise recurse until the stack overflows.
 *
 * Full reference-preservation (encoding a DAG/cyclic graph and reconstructing
 * shared identity on decode, as e.g. a `$id`/`$ref` scheme would) is out of
 * scope here — this only turns an unbounded recursion into a clear error.
 */
export class CircularReferenceError extends Error {
  constructor(public readonly path: string) {
    super(
      `encodeValue: circular reference at "${path}" (cycles are not supported; break the cycle before serializing)`,
    );
    this.name = "CircularReferenceError";
  }
}

/**
 * A registered encode/decode pair for a user or library type, keyed by a
 * stable wire tag. Distinct subclasses of a base type register under
 * distinct tags with their own `test`, so `decodeValue` reconstructs the
 * concrete subtype the tag names rather than the common base — this is the
 * codec's polymorphism resolution: which constructor runs is chosen by the
 * tag on the wire, not by a discriminator-field convention callers would
 * otherwise have to agree on separately.
 */
export interface SurrogateDescriptor<T = unknown> {
  /** Stable wire tag; must be unique across the registry and never reused for a different shape. */
  readonly tag: string;
  /** True for exactly the runtime values this surrogate should encode. */
  readonly test: (value: unknown) => boolean;
  /** Produce the plain, transport-safe fields for a matched value. */
  readonly encode: (value: T) => Record<string, unknown>;
  /** Reconstruct the runtime value from its decoded fields. */
  readonly decode: (fields: Record<string, unknown>, ctx: CodecContext) => T;
}

const surrogates = new Map<string, SurrogateDescriptor>();
// Registration order matters for `test`: later registrations are checked
// first, so a subclass registered after its base type is preferred over it.
const surrogateOrder: SurrogateDescriptor[] = [];

/** Register a surrogate encode/decode pair for a user or library type. Throws if the tag is already registered. */
export function registerSurrogate<T>(descriptor: SurrogateDescriptor<T>): void {
  if (surrogates.has(descriptor.tag)) {
    throw new Error(`registerSurrogate: tag "${descriptor.tag}" is already registered`);
  }
  surrogates.set(descriptor.tag, descriptor as SurrogateDescriptor);
  surrogateOrder.unshift(descriptor as SurrogateDescriptor);
}

/** Remove a previously registered surrogate. No-op if the tag is not registered. */
export function unregisterSurrogate(tag: string): void {
  if (surrogates.delete(tag)) {
    const i = surrogateOrder.findIndex((d) => d.tag === tag);
    if (i >= 0) surrogateOrder.splice(i, 1);
  }
}

/** Remove every registered surrogate. Mainly for test isolation. */
export function clearSurrogates(): void {
  surrogates.clear();
  surrogateOrder.length = 0;
}

function findSurrogate(value: unknown): SurrogateDescriptor | undefined {
  return surrogateOrder.find((d) => d.test(value));
}

function grainIdFields(id: GrainId): Record<string, unknown> {
  return { grainType: id.type, keyKind: id.keyKind, key: keyToString(id.key) };
}

function grainIdFrom(obj: Record<string, unknown>): GrainId {
  return GrainId.parse(
    `${obj.grainType as string}/${obj.key as string}`,
    obj.keyKind as GrainKeyKind,
  );
}

function tagged(tag: string, fields: Record<string, unknown>): Record<string, unknown> {
  return { [T]: tag, [V]: CURRENT_VERSION, ...fields };
}

/**
 * How a `Uint8Array` is carried.
 *
 * The default passes it through untouched, which is what the MessagePack serializer and the
 * in-memory clone want: msgpack has a native binary type, so tagging binary would cost a third of
 * every message body. A JSON transport has no such type — `JSON.stringify` turns a typed array into
 * `{"0":1,...}` and `JSON.parse` hands back a plain object — so the JSON paths
 * (`serializeValue` and the JSON serializer) ask for the tagged base64 form instead.
 */
export interface EncodeValueOptions {
  /** Encode `Uint8Array` as a tagged base64 envelope rather than passing it through. */
  readonly binaryAsBase64?: boolean;
}

/** Replace runtime value types with tagged, transport-safe plain forms. */
export function encodeValue(value: unknown, options: EncodeValueOptions = {}): unknown {
  return encodeInner(value, new Set<unknown>(), "$", options);
}

function encodeInner(
  value: unknown,
  seen: Set<unknown>,
  path: string,
  options: EncodeValueOptions,
): unknown {
  if (value instanceof Uint8Array) {
    // Binary (e.g. a Message body) passes through unless the caller's transport cannot carry it.
    return options.binaryAsBase64 ? tagged("bytes", { value: bytesToBase64(value) }) : value;
  }
  if (value instanceof Date) return tagged("date", { value: value.getTime() });
  if (typeof value === "bigint") return tagged("bigint", { value: value.toString() });
  if (value instanceof Guid) return tagged("guid", { value: value.toString() });
  if (value instanceof GrainId) return tagged("grainId", grainIdFields(value));
  if (value instanceof GrainCancellationToken) {
    return tagged("cancellationToken", {
      tokenId: value.tokenId,
      cancelled: value.isCancellationRequested,
    });
  }
  // A placeholder re-entering `encodeValue` (e.g. a call re-forwarded to
  // another silo, over a stale directory cache, before this silo ever bound
  // it to a live `GrainCancellationToken`) must re-tag with its wire shape —
  // without this, the generic plain-object branch below strips the `$thresh`
  // tag (only `tokenId`/`cancelled` survive as bare own properties), and the
  // next hop's `decodeValue` can no longer recognise it as a cancellation
  // token, leaving the eventual callee with a signal-less plain object.
  if (value instanceof CancellationTokenPlaceholder) {
    return tagged("cancellationToken", { tokenId: value.tokenId, cancelled: value.cancelled });
  }
  if (value instanceof SiloAddress) {
    return tagged("silo", {
      podName: value.podName,
      podUid: value.podUid,
      endpoint: value.endpoint,
    });
  }
  const ref = grainReferenceIdentity(value);
  if (ref !== undefined) {
    return tagged("grainRef", { interfaceId: ref.interfaceId, ...grainIdFields(ref.grainId) });
  }

  const surrogate = findSurrogate(value);
  if (surrogate !== undefined) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const fields = surrogate.encode(value);
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(fields))
        out[k] = encodeInner(v, seen, `${path}.${k}`, options);
      return tagged(surrogate.tag, out);
    } finally {
      seen.delete(value);
    }
  }

  if (value instanceof Map) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const entries = [...value.entries()].map(([k, v], i) => [
        encodeInner(k, seen, `${path}[${i}].key`, options),
        encodeInner(v, seen, `${path}[${i}].value`, options),
      ]);
      return tagged("map", { entries });
    } finally {
      seen.delete(value);
    }
  }
  if (value instanceof Set) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const values = [...value.values()].map((v, i) =>
        encodeInner(v, seen, `${path}[${i}]`, options),
      );
      return tagged("set", { values });
    } finally {
      seen.delete(value);
    }
  }
  if (Array.isArray(value)) {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      return value.map((v, i) => encodeInner(v, seen, `${path}[${i}]`, options));
    } finally {
      seen.delete(value);
    }
  }
  if (value !== null && typeof value === "object") {
    if (seen.has(value)) throw new CircularReferenceError(path);
    seen.add(value);
    try {
      const out: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(value))
        out[k] = encodeInner(v, seen, `${path}.${k}`, options);
      return out;
    } finally {
      seen.delete(value);
    }
  }
  return value;
}

/** Reverse `encodeValue`, rehydrating value types (and, optionally, grain refs). */
export function decodeValue(value: unknown, ctx: CodecContext = {}): unknown {
  if (value instanceof Uint8Array) return value;
  if (Array.isArray(value)) return value.map((v) => decodeValue(v, ctx));
  if (value === null || typeof value !== "object") return value;

  const obj = value as Record<string, unknown>;
  const tag = obj[T];
  if (typeof tag === "string") {
    switch (tag) {
      case "bigint":
        return BigInt(obj.value as string);
      case "date":
        return new Date(obj.value as number);
      case "bytes":
        return base64ToBytes(obj.value as string);
      case "guid":
        return Guid.parse(obj.value as string);
      case "grainId":
        return grainIdFrom(obj);
      case "cancellationToken":
        return new CancellationTokenPlaceholder(obj.tokenId as string, obj.cancelled as boolean);
      case "silo":
        return new SiloAddress(obj.podName as string, obj.podUid as string, obj.endpoint as string);
      case "map": {
        const entries = obj.entries as [unknown, unknown][];
        return new Map(entries.map(([k, v]) => [decodeValue(k, ctx), decodeValue(v, ctx)]));
      }
      case "set": {
        const values = obj.values as unknown[];
        return new Set(values.map((v) => decodeValue(v, ctx)));
      }
      case "grainRef": {
        const identity: GrainReferenceIdentity = {
          grainId: grainIdFrom(obj),
          interfaceId: obj.interfaceId as number,
        };
        return ctx.resolveGrainReference ? ctx.resolveGrainReference(identity) : identity;
      }
      default: {
        const surrogate = surrogates.get(tag);
        if (surrogate !== undefined) {
          const fields: Record<string, unknown> = {};
          for (const [k, v] of Object.entries(obj)) {
            if (k === T || k === V) continue;
            fields[k] = decodeValue(v, ctx);
          }
          return surrogate.decode(fields, ctx);
        }
        // Unknown tag (e.g. produced by a newer build's surrogate this
        // process has no registration for): fall through and decode as a
        // plain object rather than throwing, so an old reader on a
        // mixed-version cluster degrades to a structural value instead of
        // failing outright.
      }
    }
  }

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) out[k] = decodeValue(v, ctx);
  return out;
}

/** JSON string of a value with runtime types tagged; pair with `deserializeValue`. */
export function serializeValue(value: unknown): string {
  return JSON.stringify(encodeValue(value, { binaryAsBase64: true }));
}

/** Reverse `serializeValue`, rehydrating runtime value types. */
export function deserializeValue<T>(json: string, ctx?: CodecContext): T {
  return decodeValue(JSON.parse(json), ctx) as T;
}

const BASE64_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

// Hand-rolled rather than `btoa`/`Buffer`: the core package targets ES2022 with no DOM and no
// ambient Node types, and the encoding is part of a wire shape, so it is spelled out here and
// pinned by tests rather than inherited from whichever global happens to exist.
function bytesToBase64(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.length; i += 3) {
    const b0 = bytes[i] as number;
    const b1 = bytes[i + 1];
    const b2 = bytes[i + 2];
    out += BASE64_ALPHABET[b0 >> 2];
    out += BASE64_ALPHABET[((b0 & 0b11) << 4) | ((b1 ?? 0) >> 4)];
    out += b1 === undefined ? "=" : BASE64_ALPHABET[((b1 & 0b1111) << 2) | ((b2 ?? 0) >> 6)];
    out += b2 === undefined ? "=" : BASE64_ALPHABET[b2 & 0b111111];
  }
  return out;
}

function base64ToBytes(text: string): Uint8Array {
  const body = text.endsWith("==")
    ? text.slice(0, -2)
    : text.endsWith("=")
      ? text.slice(0, -1)
      : text;
  const bytes = new Uint8Array((body.length * 3) >> 2);
  let acc = 0;
  let bits = 0;
  let at = 0;
  for (const ch of body) {
    const digit = BASE64_ALPHABET.indexOf(ch);
    if (digit < 0) continue;
    acc = (acc << 6) | digit;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      bytes[at++] = (acc >> bits) & 0xff;
    }
  }
  return bytes;
}
