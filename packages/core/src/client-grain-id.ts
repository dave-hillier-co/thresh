import { GrainId } from "./grain-id";
import type { GrainType } from "./grain-type";
import { Guid } from "./guid";

/**
 * Reserved grain type for client-hosted callback objects, mirroring Orleans'
 * `GrainTypePrefix.ClientGrainType` (there: "sys.client"). A client is
 * addressed as a `GrainId` of this type with a string key; an observer is a
 * client whose key carries an extra `+scope` suffix (see below).
 */
export const CLIENT_GRAIN_TYPE: GrainType = "$client";

/** True if `type` names the client grain type (or a sub-prefix of it). */
export function isClientGrainType(type: GrainType): boolean {
  return type === CLIENT_GRAIN_TYPE || type.startsWith(CLIENT_GRAIN_TYPE);
}

/** True if `grainId` addresses a client (or an observer scoped to one). */
export function isClient(grainId: GrainId): boolean {
  return isClientGrainType(grainId.type);
}

/** A fresh client id with a random guid-string key. */
export function createClientId(): GrainId {
  return clientId(Guid.newGuid().toString());
}

/** A deterministic client id for the given key. */
export function clientId(key: string): GrainId {
  return new GrainId(CLIENT_GRAIN_TYPE, key);
}

/**
 * The separator between a client id's key and the client-scoped observer id
 * portion, mirroring Orleans' `ObserverGrainId.SegmentSeparator`.
 */
const SEGMENT_SEPARATOR = "+";

function requireClient(client: GrainId): void {
  if (!isClient(client)) {
    throw new Error(`not a client grain id: ${client.toString()}`);
  }
}

function clientKeyOf(client: GrainId): string {
  const { key } = client;
  if (typeof key !== "string") {
    throw new Error(`client grain id must have a string key, got ${client.keyKind}`);
  }
  return key;
}

/** A fresh observer id, scoped to `client`, with a random guid-string scope. */
export function createObserverId(client: GrainId): GrainId {
  return observerId(client, Guid.newGuid().toString());
}

/** A deterministic observer id, scoped to `client`, for the given `scope`. */
export function observerId(client: GrainId, scope: string): GrainId {
  requireClient(client);
  const key = clientKeyOf(client);
  return new GrainId(client.type, `${key}${SEGMENT_SEPARATOR}${scope}`);
}

/** True if `grainId` is a client id carrying an observer scope suffix. */
export function isObserverGrainId(grainId: GrainId): boolean {
  return isClient(grainId) && typeof grainId.key === "string" && grainId.key.includes(SEGMENT_SEPARATOR);
}

/**
 * The client id that owns `observer` — strips the `+scope` suffix. If
 * `observer` is already a plain client id (no suffix), it is returned
 * unchanged.
 */
export function clientIdOf(observer: GrainId): GrainId {
  requireClient(observer);
  const key = clientKeyOf(observer);
  const sep = key.indexOf(SEGMENT_SEPARATOR);
  if (sep < 0) return observer;
  return new GrainId(observer.type, key.slice(0, sep));
}

/** The scope portion of an observer id (the part after `+`). */
export function scopeOf(observer: GrainId): string {
  const key = clientKeyOf(observer);
  const sep = key.indexOf(SEGMENT_SEPARATOR);
  if (sep < 0) throw new Error(`not an observer grain id: ${observer.toString()}`);
  return key.slice(sep + 1);
}
