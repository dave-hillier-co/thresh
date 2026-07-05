import type { GrainId } from "./grain-id";

/**
 * Marker exposed by grain-reference proxies so the serializer can recognise one
 * and reduce it to its identity on the wire (rehydrated as a proxy on receive).
 */
export const GRAIN_REF: unique symbol = Symbol.for("tsva.grainRef");

export interface GrainReferenceIdentity {
  grainId: GrainId;
  interfaceId: number;
}

export function grainReferenceIdentity(value: unknown): GrainReferenceIdentity | undefined {
  if (typeof value !== "object" || value === null) return undefined;
  // Read the property directly rather than `GRAIN_REF in value`: grain
  // references are Proxies with only a `get` trap, so `in` falls back to the
  // (empty) target's default `has` and always reports false.
  const identity = (value as Record<symbol, unknown>)[GRAIN_REF];
  return identity === undefined ? undefined : (identity as GrainReferenceIdentity);
}
