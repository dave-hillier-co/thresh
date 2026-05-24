import type {
  MembershipService,
  MembershipSnapshot,
  SiloMember,
  SiloStatus,
} from "@tsva/core/membership";
import type { SiloAddress } from "@tsva/core/silo-address";

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
  ) {
    this.snapshot = {
      version: 1,
      silos: initial.map((address) => ({ address, status: "active" })),
    };
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
    this.publish(addresses.map((address) => ({ address, status: "active" as SiloStatus })));
  }

  addSilo(address: SiloAddress, status: SiloStatus = "active"): void {
    if (this.snapshot.silos.some((m) => m.address.equals(address))) return;
    this.publish([...this.snapshot.silos, { address, status }]);
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
