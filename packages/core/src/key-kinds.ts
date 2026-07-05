import type { CompoundKey } from "./grain-key";
import type { Guid } from "./guid";

/**
 * Marker interfaces a grain interface extends to declare its key kind, so the
 * factory can enforce the key type. The phantom field is never read.
 */
export interface GrainWithStringKey {
  readonly __key?: string;
}

export interface GrainWithIntegerKey {
  readonly __key?: bigint;
}

export interface GrainWithGuidKey {
  readonly __key?: Guid;
}

/** Orleans' `IGrainWithGuidCompoundKey`: a Guid primary key plus a string extension. */
export interface GrainWithGuidCompoundKey {
  readonly __key?: CompoundKey<Guid>;
}

/** Orleans' `IGrainWithIntegerCompoundKey`: an integer primary key plus a string extension. */
export interface GrainWithIntegerCompoundKey {
  readonly __key?: CompoundKey<bigint>;
}

/** Maps a grain interface to the key type its factory call requires. */
export type GrainKeyFor<T> = T extends GrainWithGuidCompoundKey
  ? CompoundKey<Guid>
  : T extends GrainWithIntegerCompoundKey
    ? CompoundKey<bigint>
    : T extends GrainWithIntegerKey
      ? bigint
      : T extends GrainWithGuidKey
        ? Guid
        : string;
