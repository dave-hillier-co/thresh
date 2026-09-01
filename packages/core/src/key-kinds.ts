import type { CompoundKey, GrainKey as RuntimeGrainKey, GrainKeyKind } from "./grain-key";
import type { Guid } from "./guid";

/**
 * Phantom marker a grain interface/type alias intersects with to declare the
 * key type accepted by `getGrain`. This is the TypeScript-first form: keep the
 * message surface as a plain structural type and add `GrainKey<TKey>` for the
 * identity shape. The phantom field is never read.
 *
 * Where the surface is fused into a `defineGrain` definition there is no
 * separate interface to mark, so declare the kind on the definition instead —
 * `{ key: "guid" }` on `defineGrain`/`defineGrainInterface`. The two paths
 * agree: a marked interface resolves to the same kind via `KeyKindFromMarker`.
 *
 * @example
 * type Greeter = GrainKey<string> & {
 *   greet(name: string): Promise<string>;
 * };
 */
export interface GrainKey<TKey extends RuntimeGrainKey = string> {
  readonly __key?: TKey;
}

/** Alias for `GrainKey<TKey>` when the grain surface reads better as a noun. */
export type KeyedGrain<TKey extends RuntimeGrainKey = string> = GrainKey<TKey>;

/**
 * Orleans-compatible marker names, retained for ports and existing examples.
 * Each is an alias of `GrainKey<TKey>`, so an interface that extends one
 * resolves to the same key type as an intersection would.
 */
export type GrainWithStringKey = GrainKey<string>;

export type GrainWithIntegerKey = GrainKey<bigint>;

export type GrainWithGuidKey = GrainKey<Guid>;

/** Orleans' `IGrainWithGuidCompoundKey`: a Guid primary key plus a string extension. */
export type GrainWithGuidCompoundKey = GrainKey<CompoundKey<Guid>>;

/** Orleans' `IGrainWithIntegerCompoundKey`: an integer primary key plus a string extension. */
export type GrainWithIntegerCompoundKey = GrainKey<CompoundKey<bigint>>;

/** Maps a declared key kind to the key type its factory call requires. */
export type KeyTypeOf<K extends GrainKeyKind> = K extends "integer"
  ? bigint
  : K extends "guid"
    ? Guid
    : K extends "guid-compound"
      ? CompoundKey<Guid>
      : K extends "integer-compound"
        ? CompoundKey<bigint>
        : string;

/**
 * Back-compat bridge: recovers the declared key kind from the phantom
 * `GrainKey<TKey>` marker on `T`, so an interface that only intersects or
 * extends a marker keeps resolving to the right key type with no edit.
 *
 * The compound markers are tested first so a compound-keyed interface cannot
 * degrade to its primary's kind. An interface carrying no marker at all
 * resolves to `"string"`, which relies on weak-type detection and holds only
 * because the phantom field is optional-only — see `key-kinds.test.ts`, which
 * pins both that fallback and the edges where it does not apply.
 */
export type KeyKindFromMarker<T> =
  T extends GrainKey<CompoundKey<Guid>>
    ? "guid-compound"
    : T extends GrainKey<CompoundKey<bigint>>
      ? "integer-compound"
      : T extends GrainKey<bigint>
        ? "integer"
        : T extends GrainKey<Guid>
          ? "guid"
          : "string";

/**
 * Maps a grain interface to the key type its factory call requires.
 *
 * @deprecated Use `KeyTypeOf<K>` against the definition's declared kind, which
 * also honours a `key` declared without a marker.
 */
export type GrainKeyFor<T> = KeyTypeOf<KeyKindFromMarker<T>>;
