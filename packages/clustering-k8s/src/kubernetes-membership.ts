import type { MembershipService, MembershipSnapshot, SiloMember } from "@thresh/core/membership";
import type { SiloAddress } from "@thresh/core/silo-address";
import {
  metadataFromSlices,
  siloMembersFromSlices,
  type EndpointSlice,
} from "@thresh/clustering-k8s/endpoint-slice";

/**
 * Source of EndpointSlice updates. The production implementation wraps the
 * Kubernetes API watch; tests push slices directly. `subscribe` returns an
 * unsubscribe function.
 */
export interface EndpointWatch {
  subscribe(onSlices: (slices: EndpointSlice[]) => void): () => void;
}

export interface KubernetesMembershipOptions {
  /** Named service port to use for the silo endpoint (defaults to the first). */
  portName?: string;
  /**
   * Pod labels under this prefix populate `SiloMember.metadata`, with the prefix
   * stripped from the key — e.g. `thresh.io/role=worker` with prefix
   * `"thresh.io/"` surfaces as `{ role: "worker" }`, the same shape
   * `useStaticMembership`'s metadata resolver produces. Requires a watch source
   * that resolves pod labels onto `EndpointSliceEndpoint.metadata` (see
   * `createKubernetesClientSource`'s `fetchPodLabels` option); omit to leave
   * `SiloMember.metadata` unset.
   */
  metadataLabelPrefix?: string;
}

/**
 * Membership derived from Kubernetes: every silo watches the same EndpointSlices
 * for the headless service, so views converge without a gossip protocol. Each
 * reconciliation produces a new versioned snapshot (docs/05).
 *
 * Two caveats about what that convergence and versioning actually guarantee,
 * both tracked as issue #72:
 *
 * 1. **The version is a per-silo counter, not a view identity.** Identical
 *    numbers on two silos denote unrelated views, each silo numbering its own
 *    from its own watch; tests and dev clusters agree only because they share
 *    one `StaticMembershipService` (see `test-cluster.ts`). The directory's
 *    `staleView` guard compares versions across silos — `op.version <
 *    appliedVersion` gates the "do I still own this?" rejection, and
 *    `ClusterNode.awaitView` waits for a caller's version — so it can neither
 *    detect a genuinely divergent ring (equal numbers, different rings: two
 *    partitions each holding a valid entry for one grain) nor avoid refusing a
 *    caller whose view is the same or newer (whose `refresh()` is then a no-op,
 *    leaving only the bounded retries). Making the version cluster-wide needs a
 *    shared, ordered view identity — the EndpointSlice list's `resourceVersion`,
 *    or a content-derived identity — plus a compatibility story for a rolling
 *    upgrade, where old silos send small counters and new ones do not.
 *
 * 2. **The local silo is always injected into its own view** (see `onSlices`),
 *    so one the watch has stopped reporting — drained, or removed from the
 *    service — stays in its own ring while every peer has dropped it, and the
 *    guard in (1) cannot see that either, by construction. Removing the
 *    injection is not a local change: `activeSilos` feeds placement candidates
 *    as well as the ring, and `SelfProbeWorker`'s probe stays a self-probe only
 *    while this silo is still a candidate (`PreferLocalPlacement` otherwise
 *    falls back to a peer, which answers for a silo whose dispatcher is the
 *    thing being probed — see `createSiloProbeGrainType`). What the local silo's
 *    own membership should follow, and how a silo outside its own ring places
 *    its own grains, is a membership-semantics decision.
 */
export class KubernetesMembership implements MembershipService {
  private snapshot: MembershipSnapshot = { version: 0, silos: [] };
  private waiters: Array<(s: MembershipSnapshot) => void> = [];
  private readonly unsubscribe: () => void;

  constructor(
    private readonly local: SiloAddress,
    watch: EndpointWatch,
    private readonly options: KubernetesMembershipOptions = {},
  ) {
    this.unsubscribe = watch.subscribe((slices) => this.onSlices(slices));
  }

  current(): MembershipSnapshot {
    return this.snapshot;
  }

  localSilo(): SiloAddress {
    return this.local;
  }

  async *updates(): AsyncIterableIterator<MembershipSnapshot> {
    for (;;) {
      yield await new Promise<MembershipSnapshot>((resolve) => this.waiters.push(resolve));
    }
  }

  stop(): void {
    this.unsubscribe();
  }

  private onSlices(slices: EndpointSlice[]): void {
    // Always include the local silo, forced `active` whatever its own endpoint
    // reports — `dedupeMembers` carries the bootstrap rationale. That the
    // injection is unconditional is caveat 2 in the class doc (issue #72): a
    // silo the watch has dropped keeps itself in its own ring.
    const members = dedupeMembers(this.local, [
      { address: this.local, status: "active" },
      ...siloMembersFromSlices(slices, this.options.portName),
    ]);
    const metadataByUid = metadataFromSlices(slices, this.options.metadataLabelPrefix);
    const silos: SiloMember[] = members.map((member) => {
      const metadata = metadataByUid.get(member.address.podUid);
      return metadata !== undefined ? { ...member, metadata } : member;
    });
    this.snapshot = { version: this.snapshot.version + 1, silos };
    const waiters = this.waiters;
    this.waiters = [];
    for (const resolve of waiters) resolve(this.snapshot);
  }
}

/**
 * One member per pod incarnation (`podName#podUid`). The local silo is always
 * included and always `active`, whatever its own endpoint reports: it is a
 * member of its own cluster view even before that endpoint shows ready (it
 * isn't ready until it can serve, and the readiness probe gates on membership
 * being healthy — including self breaks that bootstrap cycle), and a membership
 * view that told a silo it was draining would take it out of its own ring while
 * it was still serving. A transient empty watch is covered by the same rule:
 * the local silo never believes the whole cluster — itself included — vanished.
 */
function dedupeMembers(local: SiloAddress, members: readonly SiloMember[]): SiloMember[] {
  const seen = new Map<string, SiloMember>();
  for (const member of members) {
    seen.set(`${member.address.podName}#${member.address.podUid}`, member);
  }
  return [...seen.values()].map((member) =>
    member.address.equals(local) ? { ...member, status: "active" } : member,
  );
}
