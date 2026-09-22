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
