import { stableHash32 } from "./hash";
import type { InvokeMethodOptions } from "./invoke-options";

/**
 * A registered grain interface — now a *compile-time view* of a grain's message
 * surface.
 * It carries a stable id (used to route a `getGrain` to the hosting grain type
 * and to rehydrate grain references) and the per-method invocation options.
 *
 * There is no method table: a call dispatches by **method name** on the wire, so
 * the interface is purely the TypeScript shape `T` plus the handful of methods
 * that need non-default invocation options. Nothing here is generated.
 */
export interface GrainInterface<T> {
  readonly id: number;
  readonly name: string;
  /**
   * Interface version (defaults to 1). The `id` is name-derived and stable
   * across versions — two versions of one interface share an id — so a caller's
   * version travels separately on the wire for version-aware placement.
   */
  readonly version: number;
  readonly options: Readonly<Record<string, InvokeMethodOptions>>;
  /** Phantom marker so a `GrainInterface<T>` carries its interface type. */
  readonly __t?: T;
}

export interface GrainInterfaceDefinition<T> {
  /** Per-method invocation flags; only the non-default methods need an entry. */
  options?: Partial<Record<keyof T & string, InvokeMethodOptions>>;
  /** Interface version for rolling upgrades (defaults to 1). */
  version?: number;
}

// Process-wide registry so a receiving silo can resolve an interfaceId back to
// its options (and rehydrate references) without the caller's static type.
const registry = new Map<number, GrainInterface<unknown>>();

export function defineGrainInterface<T>(
  name: string,
  def: GrainInterfaceDefinition<T> = {},
): GrainInterface<T> {
  const result: GrainInterface<T> = {
    id: stableHash32(name),
    name,
    version: def.version ?? 1,
    options: { ...(def.options ?? {}) } as Record<string, InvokeMethodOptions>,
  };
  registry.set(result.id, result as GrainInterface<unknown>);
  return result;
}

export function getGrainInterface(id: number): GrainInterface<unknown> | undefined {
  return registry.get(id);
}
