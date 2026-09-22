import type { SiloAddress } from "./silo-address";

export type SiloStatus = "joining" | "active" | "draining" | "dead";

export interface SiloMember {
  address: SiloAddress;
  status: SiloStatus;
  /** Static metadata the silo advertises (e.g. `{ role: "worker" }`), for metadata-aware placement. */
  metadata?: Readonly<Record<string, string>>;
}

/** A versioned snapshot of the live silo set; `version` is the view number. */
export interface MembershipSnapshot {
  version: number;
  silos: ReadonlyArray<SiloMember>;
}

/** The live silo set, derived from Kubernetes in production (see docs/05). */
export interface MembershipService {
  current(): MembershipSnapshot;
  updates(): AsyncIterable<MembershipSnapshot>;
  localSilo(): SiloAddress;
}

/** The silos that may host or own grains: those currently `active`. */
export function activeSilos(snapshot: MembershipSnapshot): SiloAddress[] {
  return snapshot.silos.filter((m) => m.status === "active").map((m) => m.address);
}

/**
 * The silos still present in the view, whatever their status — `draining` (and a
 * not-yet-`active` joiner) included, `dead` and an endpoint removed outright
 * excluded.
 *
 * This is deliberately weaker than `activeSilos`, and is the liveness a
 * directory entry is judged by. A silo that has stopped taking new placements
 * is still running the activations already on it: graceful shutdown flips
 * readiness and only then waits out its grace period (see
 * `GracefulShutdown`), and a readiness probe can miss on a silo that is
 * perfectly healthy. Judging entries by `active` deletes the pointer to a live
 * activation the moment a readiness bit flips, and the next call builds a
 * second activation of a grain that never stopped running. Orleans removes an
 * entry once a silo's liveness is lost, not on a single readiness observation.
 */
export function memberSilos(snapshot: MembershipSnapshot): SiloAddress[] {
  return snapshot.silos.filter((m) => m.status !== "dead").map((m) => m.address);
}
