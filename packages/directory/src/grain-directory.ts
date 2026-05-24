import type { GrainAddress } from "@tsva/core/grain-address";
import type { GrainId } from "@tsva/core/grain-id";
import type { SiloAddress } from "@tsva/core/silo-address";

/**
 * The distributed grain directory: where each grain's activation currently
 * lives. `register` is compare-and-set so concurrent activations of the same
 * grain converge on a single winner; the loser forwards to it.
 */
export interface GrainDirectory {
  lookup(grainId: GrainId): Promise<GrainAddress | undefined>;
  /** CAS register; returns the winning address (the existing one if a race lost). */
  register(addr: GrainAddress, previous?: GrainAddress): Promise<GrainAddress>;
  unregister(addr: GrainAddress): Promise<void>;
  /** Bulk-remove every entry pointing at a silo that has left. */
  unregisterSilo(silo: SiloAddress): Promise<void>;
}
