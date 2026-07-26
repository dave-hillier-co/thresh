import type { GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";

/**
 * Reaches the directory partition on a remote silo. The silo supplies an
 * implementation that carries these operations over the transport; the
 * distributed directory uses it for grains it does not own locally.
 */
export interface DirectoryPeer {
  lookup(owner: SiloAddress, grainId: GrainId): Promise<GrainAddress | undefined>;
  register(owner: SiloAddress, addr: GrainAddress, previous?: GrainAddress): Promise<GrainAddress>;
  unregister(owner: SiloAddress, addr: GrainAddress): Promise<void>;
}
