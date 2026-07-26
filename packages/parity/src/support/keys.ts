import { Guid } from "@thresh/core/guid";

/** Upstream `GetRandomGrainId()`: a fresh integer key per test so tests sharing one cluster stay isolated. */
export function randomIntegerKey(): bigint {
  return BigInt(Math.floor(Math.random() * Number.MAX_SAFE_INTEGER));
}

/** Upstream `Guid.NewGuid()` grain keys. */
export function randomGuidKey(): Guid {
  return Guid.newGuid();
}
