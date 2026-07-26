import type { GrainId } from "./grain-id";
import type { GrainInterface } from "./grain-interface";

/**
 * Marker exposed by grain-reference proxies so the serializer can recognise one
 * and reduce it to its identity on the wire (rehydrated as a proxy on receive).
 */
export const GRAIN_REF: unique symbol = Symbol.for("thresh.grainRef");

/**
 * Marker exposed by grain-reference proxies for runtime re-typing — the
 * analogue of Orleans' `GrainReference.AsReference<T>()` /
 * `IInternalGrainFactory.Cast`. A proxy's `get` trap answers this symbol with
 * a function that rebuilds a reference to the SAME grain id under a different
 * `GrainInterface<T>` token; see `packages/runtime/src/grain-factory.ts`.
 */
export const GRAIN_REF_CAST: unique symbol = Symbol.for("thresh.grainRefCast");

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

/**
 * Re-type an existing grain reference to a different `GrainInterface<T>`,
 * keeping the same grain id (Orleans' `AsReference<T>`/`Cast`). The cast
 * itself is optimistic — a grain reference carries no record of every
 * interface its concrete grain type implements, so casting to an interface
 * the grain does NOT implement still succeeds here; calling a method the
 * grain doesn't have then fails at the call site (Orleans parity: casts
 * validate lazily, at resolution/call time, except for a target that isn't
 * even a registered grain interface, which fails eagerly here).
 */
export function castGrainReference<T>(value: unknown, def: GrainInterface<T>): T {
  if (typeof value !== "object" || value === null) {
    throw new TypeError("castGrainReference: value is not a grain reference");
  }
  const cast = (value as Record<symbol, unknown>)[GRAIN_REF_CAST];
  if (typeof cast !== "function") {
    throw new TypeError("castGrainReference: value is not a grain reference");
  }
  return (cast as (def: GrainInterface<T>) => T)(def);
}
