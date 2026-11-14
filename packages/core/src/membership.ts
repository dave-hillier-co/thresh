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
 * `version` is a cluster-wide view identity, ordered: two silos holding the same
 * view hold the same version, and a silo that has not yet observed a change holds
 * a lower one than a silo that has. `DistributedGrainDirectory` and `ClusterNode`
 * rely on exactly that across silos — the `staleView` rejection of a caller whose
 * version is behind, and `awaitView` waiting for a caller's version to arrive.
 *
 * Where it comes from depends on the provider:
 *
 * - **Kubernetes** (`KubernetesMembership`): the highest EndpointSlice
 *   `resourceVersion` the silo's watch has observed — an etcd revision, one
 *   cluster-wide increasing counter — so silos that have seen the same writes to the
 *   service's slices agree on it without coordinating (issue #72; see
 *   `WatchedEndpoints` and `docs/deviations.md`).
 * - **Static/test** (`StaticMembershipService`): a counter on the one shared
 *   instance, which is a single authority because every silo of an in-process
 *   cluster reads it.
 *
 * A provider whose views are not derived from one shared, ordered source cannot
 * honour this contract, and a counter kept per silo does not (it was issue #72's
 * defect: identical views on two silos numbered 4 and 2 made one wait on the other
 * until its calls timed out).
 */
export interface MembershipSnapshot {
  version: number;
  silos: ReadonlyArray<SiloMember>;
}

/** The live silo set, derived from Kubernetes in production (see docs/05). */
export interface MembershipService {
  current(): MembershipSnapshot;
  /**
   * The views published from now on, with latest-value semantics: an iterator that
   * is behind `current()` yields it on its next step, whether or not it was waiting
   * when the view was published, so a consumer that is busy when views change never
   * loses one. Intermediate views may be coalesced; the latest is never skipped.
   * (Orleans `AsyncEnumerable<T>`, which `ClusterMembershipService.MembershipUpdates`
   * is built on.)
   */
  updates(): AsyncIterable<MembershipSnapshot>;
  localSilo(): SiloAddress;
}

/**
 * A latest-value feed of membership snapshots: the publishing half a
 * `MembershipService` implementation keeps, and the `updates()` iterators it hands
 * out. Mirrors Orleans `AsyncEnumerable<T>` (`src/Orleans.Core/Utils/AsyncEnumerable.cs`)
 * in the property that matters — a publish is never lost because a consumer was busy,
 * since each iterator compares its position against the latest snapshot rather than
 * waiting for a one-shot wake-up — but coalesces: an iterator that fell several
 * views behind yields only the latest, where Orleans walks the whole chain. Every
 * consumer here re-reads `current()` to apply a view, so the intermediate views it
 * would skip are ones it could not have applied anyway.
 *
 * An iterator starts positioned at the snapshot current when `updates()` was
 * called: it yields views published after that, not the one already in force.
 */
export class MembershipFeed {
  private snapshot: MembershipSnapshot;
  private waiters: Array<() => void> = [];

  constructor(initial: MembershipSnapshot) {
    this.snapshot = initial;
  }

  current(): MembershipSnapshot {
    return this.snapshot;
  }

  publish(snapshot: MembershipSnapshot): void {
    this.snapshot = snapshot;
    const waiters = this.waiters;
    this.waiters = [];
    for (const wake of waiters) wake();
  }

  updates(): AsyncIterableIterator<MembershipSnapshot> {
    let seen = this.snapshot;
    const iterator: AsyncIterableIterator<MembershipSnapshot> = {
      [Symbol.asyncIterator]: () => iterator,
      next: async (): Promise<IteratorResult<MembershipSnapshot>> => {
        while (this.snapshot === seen) {
          await new Promise<void>((resolve) => this.waiters.push(resolve));
        }
        seen = this.snapshot;
        return { value: seen, done: false };
      },
    };
    return iterator;
  }
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
