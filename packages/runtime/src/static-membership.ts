import type {
  MembershipService,
  MembershipSnapshot,
  SiloMember,
  SiloStatus,
} from "@thresh/core/membership";
import type { SiloAddress } from "@thresh/core/silo-address";

/**
 * A membership service backed by an explicit, mutable silo list. Used for local
 * development and multi-silo tests; Kubernetes-derived membership replaces it in
 * Phase 3. Mutating the view bumps the version and pushes the new snapshot to
 * `updates()` subscribers, mirroring how a real view change propagates.
 */
export class StaticMembershipService implements MembershipService {
  private snapshot: MembershipSnapshot;
  private waiters: Array<(s: MembershipSnapshot) => void> = [];

  constructor(
    private readonly local: SiloAddress,
    initial: readonly SiloAddress[],
    private readonly metadata?: (silo: SiloAddress) => Readonly<Record<string, string>> | undefined,
  ) {
    this.snapshot = {
      version: 1,
      silos: initial.map((address) => this.member(address, "active")),
    };
  }

  /** Build a member, attaching the resolver's metadata for the silo when present. */
  private member(address: SiloAddress, status: SiloStatus): SiloMember {
    const meta = this.metadata?.(address);
    return { address, status, ...(meta !== undefined ? { metadata: meta } : {}) };
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

  /** Replace the whole view (all `active`), bumping the version. */
  setSilos(addresses: readonly SiloAddress[]): void {
    this.publish(addresses.map((address) => this.member(address, "active")));
  }

  addSilo(
    address: SiloAddress,
    status: SiloStatus = "active",
    metadata?: Readonly<Record<string, string>>,
  ): void {
    if (this.snapshot.silos.some((m) => m.address.equals(address))) return;
    const member =
      metadata !== undefined ? { address, status, metadata } : this.member(address, status);
    this.publish([...this.snapshot.silos, member]);
  }

  removeSilo(address: SiloAddress): void {
    this.publish(this.snapshot.silos.filter((m) => !m.address.equals(address)));
  }

  setStatus(address: SiloAddress, status: SiloStatus): void {
    this.publish(
      this.snapshot.silos.map((m) => (m.address.equals(address) ? { ...m, status } : m)),
    );
  }

  private publish(silos: readonly SiloMember[]): void {
    this.snapshot = { version: this.snapshot.version + 1, silos };
    const waiters = this.waiters;
    this.waiters = [];
    for (const resolve of waiters) resolve(this.snapshot);
  }
}
