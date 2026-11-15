import {
  MembershipFeed,
  type MembershipService,
  type MembershipSnapshot,
  type SiloMember,
  type SiloStatus,
} from "@thresh/core/membership";
import type { SiloAddress } from "@thresh/core/silo-address";

/**
 * A membership service backed by an explicit, mutable silo list. Used for local
 * development and multi-silo tests; Kubernetes-derived membership replaces it in
 * Phase 3. Mutating the view bumps the version and publishes the new snapshot to
 * `updates()` iterators with latest-value semantics (see `MembershipFeed`),
 * mirroring how a real view change propagates.
 */
export class StaticMembershipService implements MembershipService {
  private readonly feed: MembershipFeed;

  constructor(
    private readonly local: SiloAddress,
    initial: readonly SiloAddress[],
    private readonly metadata?: (silo: SiloAddress) => Readonly<Record<string, string>> | undefined,
  ) {
    this.feed = new MembershipFeed({
      version: 1,
      silos: initial.map((address) => this.member(address, "active")),
    });
  }

  /** Build a member, attaching the resolver's metadata for the silo when present. */
  private member(address: SiloAddress, status: SiloStatus): SiloMember {
    const meta = this.metadata?.(address);
    return { address, status, ...(meta !== undefined ? { metadata: meta } : {}) };
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

  /**
   * One shared view, so there is nothing to catch up on: resolves at once. A static view counts
   * as listed and synced from the start.
   */
  refresh(_version: number): Promise<void> {
    return Promise.resolve();
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
    if (this.current().silos.some((m) => m.address.equals(address))) return;
    const member =
      metadata !== undefined ? { address, status, metadata } : this.member(address, status);
    this.publish([...this.current().silos, member]);
  }

  removeSilo(address: SiloAddress): void {
    this.publish(this.current().silos.filter((m) => !m.address.equals(address)));
  }

  setStatus(address: SiloAddress, status: SiloStatus): void {
    this.publish(
      this.current().silos.map((m) => (m.address.equals(address) ? { ...m, status } : m)),
    );
  }

  private publish(silos: readonly SiloMember[]): void {
    this.feed.publish({ version: this.current().version + 1, silos });
  }
}
