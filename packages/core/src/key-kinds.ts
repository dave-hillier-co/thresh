import type { CompoundKey, GrainKey as RuntimeGrainKey } from "./grain-key";
import type { Guid } from "./guid";

/**
 * Phantom marker a grain interface/type alias intersects with to declare the
 * key type accepted by `getGrain`. This is the TypeScript-first form: keep the
 * message surface as a plain structural type and add `GrainKey<TKey>` for the
 * identity shape.
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

/** Orleans-compatible marker names retained for ports and existing examples. */
export interface GrainWithStringKey extends GrainKey<string> {}

export interface GrainWithIntegerKey extends GrainKey<bigint> {}

export interface GrainWithGuidKey extends GrainKey<Guid> {}

/** Orleans' `IGrainWithGuidCompoundKey`: a Guid primary key plus a string extension. */
export interface GrainWithGuidCompoundKey extends GrainKey<CompoundKey<Guid>> {}

/** Orleans' `IGrainWithIntegerCompoundKey`: an integer primary key plus a string extension. */
export interface GrainWithIntegerCompoundKey extends GrainKey<CompoundKey<bigint>> {}

/** Maps a grain interface to the key type its factory call requires. */
export type GrainKeyFor<T> = T extends GrainKey<infer TKey> ? TKey : string;
