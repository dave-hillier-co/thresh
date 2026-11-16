import type { ActivationId } from "./activation-id";
import type { GrainId } from "./grain-id";
import type { SiloAddress } from "./silo-address";

/** Where a grain's activation currently lives — an ephemeral directory entry. */
export interface GrainAddress {
  grainId: GrainId;
  silo: SiloAddress;
  activationId: ActivationId;
  /**
   * The view version of the directory partition that registered this entry (Orleans
   * `GrainAddress.MembershipVersion`), stamped by the owner. Absent on an address that has not
   * been registered yet. Not part of an address's identity: `grainAddressEquals` ignores it.
   */
  version?: number;
}

export function grainAddressEquals(a: GrainAddress, b: GrainAddress): boolean {
  return a.grainId.equals(b.grainId) && a.silo.equals(b.silo) && a.activationId === b.activationId;
}
