import type { GrainReferenceIdentity } from "@tsva/core/grain-reference";

/** Pluggable codec for grain method arguments, results and state. */
export interface Serializer {
  serialize(value: unknown): Uint8Array;
  deserialize<T>(bytes: Uint8Array): T;
}

export interface SerializerOptions {
  /** Rehydrate grain references encountered while deserializing. */
  resolveGrainReference?: (identity: GrainReferenceIdentity) => unknown;
}
