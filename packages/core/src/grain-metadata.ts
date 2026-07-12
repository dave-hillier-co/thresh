import type { GrainType } from "./grain-type";

/**
 * A serializable description of a placement filter declared on a grain. Kept inert
 * here so `@tsva/core` need not depend on runtime placement classes; the runtime
 * resolves it to a `PlacementFilter` instance. A grain may declare several
 * (Orleans' stackable `[XyzPlacementFilter(order)]` attributes); `order`
 * (defaulting to `0`) fixes the composition order — ascending, output of one
 * feeding the next — and two descriptors on the same grain sharing an `order`
 * is a configuration error the runtime rejects at placement time (Orleans
 * `InvalidOperationException`).
 */
export type PlacementFilterDescriptor =
  | { kind: "metadataMatch"; match: Readonly<Record<string, string>>; order?: number }
  | { kind: "requiredMatchSiloMetadata"; keys: readonly string[]; order?: number }
  | {
      kind: "preferredMatchSiloMetadata";
      keys: readonly string[];
      minCandidates?: number;
      order?: number;
    }
  /**
   * A custom filter director registered on the silo builder via
   * `addPlacementFilter(name, director)` (Orleans
   * `AddPlacementFilter<TStrategy, TDirector>`), resolved by `name` from the
   * `PlacementFilterRegistry` at placement time.
   */
  | { kind: "custom"; name: string; order?: number };

export interface GrainOptions {
  /** Override the grain type name (defaults to the class name without "Grain"). */
  name?: string;
  placement?: "random" | "preferLocal" | "activationCount" | "siloRole" | "resourceOptimized";
  /** The silo role required by `placement: "siloRole"`. */
  role?: string;
  /** Filters that prune candidate silos by metadata before the placement strategy runs. */
  placementFilters?: readonly PlacementFilterDescriptor[];
  stateless?: boolean;
  collectionAgeSeconds?: number;
}

export interface GrainMetadata {
  grainType: GrainType;
  options: GrainOptions;
  reentrant: boolean;
  /**
   * Stream namespaces this grain type is implicitly subscribed to (Orleans'
   * `[ImplicitStreamSubscription]`). A grain of this type with key `K` is
   * auto-subscribed to the stream `(namespace, K)`, with no explicit `subscribe`.
   */
  implicitSubscriptions: readonly string[];
  /**
   * Broadcast-channel namespaces this grain type is implicitly subscribed to
   * (Orleans' `[ImplicitChannelSubscription]`). A grain of this type with key `K`
   * receives every item published to the channel `(namespace, K)`. Tracked
   * separately from `implicitSubscriptions` — broadcast channels and streams are
   * distinct subsystems in Orleans.
   */
  broadcastSubscriptions: readonly string[];
  /** `[MayInterleave]`-equivalent predicate, if this grain type declared one. */
  mayInterleave?: MayInterleavePredicate;
}

export type GrainConstructor = abstract new (...args: never[]) => object;

/**
 * Orleans' `[MayInterleave]` predicate: given the incoming call's method name
 * (and its arguments), decides whether that call may interleave with the
 * currently-running turn on the same activation — even though the grain is
 * not fully `@reentrant()`. Mirrors `IMayInterleavePredicate`/`MayInterleave`.
 */
export type MayInterleavePredicate = (methodName: string, args: readonly unknown[]) => boolean;

// Decorator order is irrelevant: options, the reentrant flag and implicit
// stream subscriptions are tracked separately and composed on read.
const optionsRegistry = new WeakMap<
  GrainConstructor,
  { grainType: GrainType; options: GrainOptions }
>();
const reentrantRegistry = new WeakSet<GrainConstructor>();
const implicitSubscriptionRegistry = new WeakMap<GrainConstructor, Set<string>>();
const broadcastSubscriptionRegistry = new WeakMap<GrainConstructor, Set<string>>();
const mayInterleaveRegistry = new WeakMap<GrainConstructor, MayInterleavePredicate>();

export function setGrainOptions(
  ctor: GrainConstructor,
  grainType: GrainType,
  options: GrainOptions,
): void {
  optionsRegistry.set(ctor, { grainType, options });
}

export function markReentrant(ctor: GrainConstructor): void {
  reentrantRegistry.add(ctor);
}

/** Record this grain type's `[MayInterleave]`-equivalent admission predicate. */
export function markMayInterleave(ctor: GrainConstructor, predicate: MayInterleavePredicate): void {
  mayInterleaveRegistry.set(ctor, predicate);
}

function markSubscription(
  registry: WeakMap<GrainConstructor, Set<string>>,
  ctor: GrainConstructor,
  namespace: string,
): void {
  let namespaces = registry.get(ctor);
  if (namespaces === undefined) {
    namespaces = new Set();
    registry.set(ctor, namespaces);
  }
  namespaces.add(namespace);
}

/** Record that this grain type is implicitly subscribed to a stream namespace. */
export function markImplicitSubscription(ctor: GrainConstructor, namespace: string): void {
  markSubscription(implicitSubscriptionRegistry, ctor, namespace);
}

/** Record that this grain type is implicitly subscribed to a broadcast-channel namespace. */
export function markBroadcastSubscription(ctor: GrainConstructor, namespace: string): void {
  markSubscription(broadcastSubscriptionRegistry, ctor, namespace);
}

export function getGrainMetadata(ctor: GrainConstructor): GrainMetadata | undefined {
  const entry = optionsRegistry.get(ctor);
  if (entry === undefined) return undefined;
  const implicit = implicitSubscriptionRegistry.get(ctor);
  const broadcast = broadcastSubscriptionRegistry.get(ctor);
  const mayInterleave = mayInterleaveRegistry.get(ctor);
  return {
    grainType: entry.grainType,
    options: entry.options,
    reentrant: reentrantRegistry.has(ctor),
    implicitSubscriptions: implicit === undefined ? [] : [...implicit],
    broadcastSubscriptions: broadcast === undefined ? [] : [...broadcast],
    ...(mayInterleave !== undefined ? { mayInterleave } : {}),
  };
}

export function defaultGrainType(className: string): GrainType {
  const suffix = "Grain";
  return className.endsWith(suffix) && className.length > suffix.length
    ? className.slice(0, -suffix.length)
    : className;
}
