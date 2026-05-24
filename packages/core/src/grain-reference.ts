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
  if (typeof value === "object" && value !== null && GRAIN_REF in value) {
    return (value as Record<symbol, unknown>)[GRAIN_REF] as GrainReferenceIdentity;
  }
  return undefined;
}
