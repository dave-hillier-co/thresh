import type { GrainKeyKind } from "./grain-key";
import { stableHash32 } from "./hash";
import type { InvokeMethodOptions } from "./invoke-options";
import type { KeyKindFromMarker } from "./key-kinds";

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
export interface GrainInterface<T, K extends GrainKeyKind = GrainKeyKind> {
  readonly id: number;
  readonly name: string;
  /**
   * Interface version (defaults to 1). The `id` is name-derived and stable
   * across versions — two versions of one interface share an id — so a caller's
   * version travels separately on the wire for version-aware placement.
   */
  readonly version: number;
  readonly options: Readonly<Record<string, InvokeMethodOptions>>;
  /**
   * Marks this as a `GrainExtension` interface (Orleans `IGrainExtension`): an
   * orthogonal method surface dispatched by interface rather than on the
   * grain instance, bound per-activation via `GrainRuntime.getOrSetExtension`.
   * Defaults to `false` for an ordinary grain interface.
   */
  readonly extension?: boolean;
  /**
   * The declared key kind, when it was declared as a value. Absent when the kind
   * comes from the type system alone — a legacy `GrainWith*Key` phantom marker
   * on `T`, or the `K` type argument — neither of which survives erasure. `K` is
   * always authoritative for callers; this is evidence for the runtime.
   */
  readonly key?: GrainKeyKind;
  /**
   * The implementation constructor, when this definition is a *fused* one — a
   * `defineGrain` that is its own interface. Absent on a hand-written
   * `defineGrainInterface`, which declares a contract and nothing else. The
   * registry uses it to tell a legitimate pairing (a declared interface and the
   * `defineGrain` implementing it) from two grain types fused under one name,
   * which would otherwise silently share one wire id. See `GrainDefinition`,
   * which narrows it to a `Grain` subclass.
   */
  readonly grain?: new () => object;
  /** Phantom marker so a `GrainInterface<T>` carries its interface type. */
  readonly __t?: T;
  /** Phantom marker for the key kind: `key` is absent on the marker path. */
  readonly __k?: K;
}

/** The erased form: a definition of any interface type and any key kind. */
export type AnyGrainInterface = GrainInterface<unknown, GrainKeyKind>;

export interface GrainInterfaceDefinition<T, K extends GrainKeyKind = KeyKindFromMarker<T>> {
  /** Per-method invocation flags; only the non-default methods need an entry. */
  options?: Partial<Record<keyof T & string, InvokeMethodOptions>>;
  /** Interface version for rolling upgrades (defaults to 1). */
  version?: number;
  /** See `GrainInterface.extension`. Defaults to `false`. */
  extension?: boolean;
  /**
   * The key kind callers must address this grain with. Defaults to the kind
   * implied by `T`'s legacy phantom marker, or `"string"` when it has none.
   *
   * State the kind exactly once. Since `T` has to be given explicitly here (an
   * interface has no implementation to infer it from) and TypeScript has no
   * partial type-argument inference, the place to state it is the second type
   * argument — `defineGrainInterface<ICounter, "integer">("ICounter")`. This
   * option exists for the inferred paths (`defineGrain`) and to leave runtime
   * evidence; where both are given they must agree, and a disagreement with a
   * same-named definition already in the registry throws.
   */
  key?: K;
}

// Process-wide registry so a receiving silo can resolve an interfaceId back to
// its options (and rehydrate references) without the caller's static type.
const registry = new Map<number, AnyGrainInterface>();

export interface RegisterInterfaceOptions {
  /**
   * Fold this definition into an existing entry for the same id instead of
   * replacing it. One name is legitimately defined more than once — an
   * interface module and its implementation module, or a `defineGrainInterface`
   * paired with a same-named fused `defineGrain` — and a clobber silently drops
   * whatever the surviving definition left out. Since the *receiving* silo reads
   * `extension` and `options` back out of the registry, that shows up as a
   * remote call dispatching down the wrong path, not as a local type error.
   *
   * Merged: per-method `options` (union, later definition wins per method),
   * `extension`, `key` and `grain` (inherited when the later definition omits
   * them; a disagreeing `key`, or a second *different* implementation, throws).
   * Not merged: `version`, which is per-definition —
   * one name at two versions is the rolling-upgrade path, and each caller sends
   * its own object's version on the wire.
   */
  merge?: boolean;
}

/**
 * Publish an interface under its id so a receiving silo can recover it from an
 * inbound call. Returns the entry now in the registry, which under `merge` is a
 * new object combining this definition with the one it found.
 */
export function registerInterface<T, K extends GrainKeyKind>(
  iface: GrainInterface<T, K>,
  options: RegisterInterfaceOptions = {},
): GrainInterface<T, K> {
  const existing = registry.get(iface.id);
  if (existing !== undefined && existing.name !== iface.name) {
    throw new Error(
      `grain interface id collision: "${existing.name}" and "${iface.name}" both hash to ` +
        `${iface.id}. The id is the wire identity, so the later definition would take over ` +
        `the earlier one's calls. Rename one of them.`,
    );
  }
  if (
    existing?.grain !== undefined &&
    iface.grain !== undefined &&
    existing.grain !== iface.grain
  ) {
    throw new Error(
      `grain "${iface.name}" is defined twice: two implementations are fused to the interface ` +
        `id ${iface.id}, so calls addressed to one would dispatch to the other. Give one of them ` +
        `a distinct name, or pin its \`interfaceName\` option to a distinct wire name.`,
    );
  }
  if (existing === undefined || options.merge !== true) {
    registry.set(iface.id, iface as AnyGrainInterface);
    return iface;
  }
  if (iface.key !== undefined && existing.key !== undefined && iface.key !== existing.key) {
    throw new Error(
      `grain interface "${iface.name}" is already registered with key kind ` +
        `"${existing.key}"; this definition declares "${iface.key}". One interface has one ` +
        `key kind — declare it once, on whichever definition callers import.`,
    );
  }
  const key = iface.key ?? existing.key;
  const extension = iface.extension ?? existing.extension;
  const grain = iface.grain ?? existing.grain;
  const merged: GrainInterface<T, K> = {
    id: iface.id,
    name: iface.name,
    version: iface.version,
    options: { ...existing.options, ...iface.options },
    ...(extension !== undefined ? { extension } : {}),
    ...(key !== undefined ? { key } : {}),
    ...(grain !== undefined ? { grain } : {}),
  };
  registry.set(merged.id, merged as AnyGrainInterface);
  return merged;
}

/**
 * Declare (and publish) an interface under `name`.
 *
 * `grain` is for the fused path only: `defineGrain` passes the implementation it
 * just built so the registry can reject a second, different implementation of the
 * same name. Hand-written interface modules leave it off.
 */
export function defineGrainInterface<T, K extends GrainKeyKind = KeyKindFromMarker<T>>(
  name: string,
  def: GrainInterfaceDefinition<T, K> = {},
  grain?: new () => object,
): GrainInterface<T, K> {
  const declared: GrainInterface<T, K> = {
    id: stableHash32(name),
    name,
    version: def.version ?? 1,
    options: { ...(def.options ?? {}) } as Record<string, InvokeMethodOptions>,
    ...(def.extension !== undefined ? { extension: def.extension } : {}),
    ...(def.key !== undefined ? { key: def.key } : {}),
    ...(grain !== undefined ? { grain } : {}),
  };
  return registerInterface(declared, { merge: true });
}

export function getGrainInterface(id: number): AnyGrainInterface | undefined {
  return registry.get(id);
}
