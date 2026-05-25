import { GrainId } from "./grain-id";
import { keyToString, type GrainKeyKind } from "./grain-key";
import { grainReferenceIdentity, type GrainReferenceIdentity } from "./grain-reference";
import { Guid } from "./guid";
import { SiloAddress } from "./silo-address";

// Tag key for the plain, transport-safe form of a runtime value type. The JSON
// and MessagePack serializers and the durable providers share this
// transformation so what they can represent stays identical.
const T = "$tsva";

export interface CodecContext {
  /** Rehydrate a grain reference identity into a working proxy on receive. */
  resolveGrainReference?: (identity: GrainReferenceIdentity) => unknown;
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

/** Replace runtime value types with tagged, transport-safe plain forms. */
export function encodeValue(value: unknown): unknown {
  if (value instanceof Uint8Array) return value; // binary (e.g. a Message body) passes through
  if (value instanceof Date) return { [T]: "date", value: value.getTime() };
  if (typeof value === "bigint") return { [T]: "bigint", value: value.toString() };
  if (value instanceof Guid) return { [T]: "guid", value: value.toString() };
  if (value instanceof GrainId) return { [T]: "grainId", ...grainIdFields(value) };
  if (value instanceof SiloAddress) {
    return { [T]: "silo", podName: value.podName, podUid: value.podUid, endpoint: value.endpoint };
  }
  const ref = grainReferenceIdentity(value);
  if (ref !== undefined) {
    return { [T]: "grainRef", interfaceId: ref.interfaceId, ...grainIdFields(ref.grainId) };
  }
  if (Array.isArray(value)) return value.map(encodeValue);
  if (value !== null && typeof value === "object") {
    const out: Record<string, unknown> = {};
    for (const [k, v] of Object.entries(value)) out[k] = encodeValue(v);
    return out;
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
      case "guid":
        return Guid.parse(obj.value as string);
      case "grainId":
        return grainIdFrom(obj);
      case "silo":
        return new SiloAddress(obj.podName as string, obj.podUid as string, obj.endpoint as string);
      case "grainRef": {
        const identity: GrainReferenceIdentity = {
          grainId: grainIdFrom(obj),
          interfaceId: obj.interfaceId as number,
        };
        return ctx.resolveGrainReference ? ctx.resolveGrainReference(identity) : identity;
      }
    }
  }

  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(obj)) out[k] = decodeValue(v, ctx);
  return out;
}

/** JSON string of a value with runtime types tagged; pair with `deserializeValue`. */
export function serializeValue(value: unknown): string {
  return JSON.stringify(encodeValue(value));
}

/** Reverse `serializeValue`, rehydrating runtime value types. */
export function deserializeValue<T>(json: string, ctx?: CodecContext): T {
  return decodeValue(JSON.parse(json), ctx) as T;
}
