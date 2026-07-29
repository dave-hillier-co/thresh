import type { CompoundKey, GrainKeyKind } from "./grain-key";
import type { Guid } from "./guid";

/**
 * Marker interfaces a grain interface extends to declare its key kind, so the
 * factory can enforce the key type. The phantom field is never read.
 *
 * @deprecated Declare the kind on the definition instead — `{ key: "guid" }` on
 * `defineGrainInterface`. The markers stay exported and keep working: a marked
 * interface still resolves to the same key type, via `KeyKindFromMarker`.
 */
export interface GrainWithStringKey {
  readonly __key?: string;
}

/** @deprecated Declare `{ key: "integer" }` on the definition instead. */
export interface GrainWithIntegerKey {
  readonly __key?: bigint;
}

/** @deprecated Declare `{ key: "guid" }` on the definition instead. */
export interface GrainWithGuidKey {
  readonly __key?: Guid;
}

/**
 * Orleans' `IGrainWithGuidCompoundKey`: a Guid primary key plus a string extension.
 *
 * @deprecated Declare `{ key: "guid-compound" }` on the definition instead.
 */
export interface GrainWithGuidCompoundKey {
  readonly __key?: CompoundKey<Guid>;
}

/**
 * Orleans' `IGrainWithIntegerCompoundKey`: an integer primary key plus a string extension.
 *
 * @deprecated Declare `{ key: "integer-compound" }` on the definition instead.
 */
export interface GrainWithIntegerCompoundKey {
  readonly __key?: CompoundKey<bigint>;
}

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
 * Back-compat bridge: recovers the key kind from the legacy phantom marker on
 * `T`, so an interface that only `extends GrainWith*Key` keeps resolving to the
 * right key type with no edit. An unmarked interface resolves to `"string"`,
 * which relies on weak-type detection — see `key-kinds.test.ts`.
 */
export type KeyKindFromMarker<T> = T extends GrainWithGuidCompoundKey
  ? "guid-compound"
  : T extends GrainWithIntegerCompoundKey
    ? "integer-compound"
    : T extends GrainWithIntegerKey
      ? "integer"
      : T extends GrainWithGuidKey
        ? "guid"
        : "string";

/**
 * Maps a grain interface to the key type its factory call requires.
 *
 * @deprecated Use `KeyTypeOf<K>` against the definition's declared kind, which
 * also honours a `key` declared without a marker.
 */
export type GrainKeyFor<T> = KeyTypeOf<KeyKindFromMarker<T>>;
