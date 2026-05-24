import type { MembershipService, MembershipSnapshot, SiloMember } from "@tsva/core/membership";
import type { SiloAddress } from "@tsva/core/silo-address";
import { readySilosFromSlices, type EndpointSlice } from "@tsva/clustering-k8s/endpoint-slice";

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
    const ready = dedupe(readySilosFromSlices(slices, this.options.portName));
    const silos: SiloMember[] = ready.map((address) => ({ address, status: "active" }));
    this.snapshot = { version: this.snapshot.version + 1, silos };
    const waiters = this.waiters;
    this.waiters = [];
    for (const resolve of waiters) resolve(this.snapshot);
  }
}

function dedupe(silos: SiloAddress[]): SiloAddress[] {
  const seen = new Map<string, SiloAddress>();
  for (const silo of silos) seen.set(`${silo.podName}#${silo.podUid}`, silo);
  return [...seen.values()];
}
