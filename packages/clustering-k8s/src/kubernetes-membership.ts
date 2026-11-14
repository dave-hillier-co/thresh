import {
  MembershipFeed,
  type MembershipService,
  type MembershipSnapshot,
  type SiloMember,
} from "@thresh/core/membership";
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
 *
 * `revision` is the highest EndpointSlice `resourceVersion` the watch has observed
 * (see `WatchedEndpoints`), which becomes the view version. A source that reports
 * none (a hand-driven test watch) gets a local counter instead, which only means
 * anything within one silo.
 */
export interface EndpointWatch {
  subscribe(onSlices: (slices: EndpointSlice[], revision?: number) => void): () => void;
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
 * **The version is the observed EndpointSlice revision** (issue #72): the highest
 * `resourceVersion` the watch has seen across the listed slices and the watch
 * events (see `WatchedEndpoints`). Two silos that have observed the same writes to
 * the service's slices hold the same version, and a silo whose watch is behind
 * holds a lower one — the cluster-wide, ordered view identity the directory's
 * `staleView` guard and `ClusterNode.awaitView` compare across silos. A re-list
 * that brings nothing new publishes nothing.
 *
 * One caveat remains, also tracked as issue #72: **the local silo is always
 * injected into its own view** (see `onSlices`), so one the watch does not report
 * as ready — still starting, drained, or removed from the service — is in its own
 * ring while its peers' rings leave it out, under the same version. Removing the
 * injection is not a local change: `activeSilos` feeds placement candidates as
 * well as the ring, and `SelfProbeWorker`'s probe stays a self-probe only while
 * this silo is still a candidate (`PreferLocalPlacement` otherwise falls back to
 * a peer, which answers for a silo whose dispatcher is the thing being probed —
 * see `createSiloProbeGrainType`). What the local silo's own membership should
 * follow, and how a silo outside its own ring places its own grains, is a
 * membership-semantics decision.
 */
export class KubernetesMembership implements MembershipService {
  private readonly feed = new MembershipFeed({ version: 0, silos: [] });
  private readonly unsubscribe: () => void;

  constructor(
    private readonly local: SiloAddress,
    watch: EndpointWatch,
    private readonly options: KubernetesMembershipOptions = {},
  ) {
    this.unsubscribe = watch.subscribe((slices, revision) => this.onSlices(slices, revision));
  }

  current(): MembershipSnapshot {
    return this.feed.current();
  }

  localSilo(): SiloAddress {
    return this.local;
  }

  updates(): AsyncIterableIterator<MembershipSnapshot> {
    return this.feed.updates();
  }

  stop(): void {
    this.unsubscribe();
  }

  private onSlices(slices: EndpointSlice[], revision: number | undefined): void {
    const previous = this.feed.current();
    // The same revision is the same set of slice writes, so the same view: nothing to
    // publish (a re-list after a reconnect that missed nothing).
    if (revision !== undefined && revision === previous.version && previous.silos.length > 0) {
      return;
    }
    // Always include the local silo, forced `active` whatever its own endpoint
    // reports — `dedupeMembers` carries the bootstrap rationale. That the
    // injection is unconditional is the caveat in the class doc (issue #72): a
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
    const version =
      revision !== undefined ? Math.max(revision, previous.version) : previous.version + 1;
    this.feed.publish({ version, silos });
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
