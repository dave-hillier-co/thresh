import type { SiloAddress } from "./silo-address";

export type SiloStatus = "joining" | "active" | "draining" | "dead";

export interface SiloMember {
  address: SiloAddress;
  status: SiloStatus;
  /** Static metadata the silo advertises (e.g. `{ role: "worker" }`), for metadata-aware placement. */
  metadata?: Readonly<Record<string, string>>;
}

/**
 * A versioned snapshot of the live silo set.
 *
 * `version` numbers the views of ONE membership service: it is comparable only
 * between snapshots of that same service, and is otherwise opaque. In production
 * every silo has its own (each watching Kubernetes for itself), so equal versions
 * on two silos denote unrelated views; only the test/dev `StaticMembershipService`,
 * shared across an in-process cluster, is a single authority whose version means
 * the same view everywhere.
 *
 * `DistributedGrainDirectory` and `ClusterNode` nevertheless compare these
 * versions across silos — a `staleView` rejection, and `awaitView` waiting for a
 * caller's version — which asks for exactly the cluster-wide view identity this
 * interface does not promise. See issue #72.
 */
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
