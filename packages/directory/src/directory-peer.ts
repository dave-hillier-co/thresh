import type { GrainAddress } from "@thresh/core/grain-address";
import type { GrainId } from "@thresh/core/grain-id";
import type { SiloAddress } from "@thresh/core/silo-address";
import type { DirectoryResult } from "@thresh/directory/grain-directory-partition";

/**
 * Reaches the directory partition on a remote silo. The silo supplies an implementation that
 * carries these operations over the transport; the distributed directory uses it for grains it
 * does not own locally. Every operation carries the caller's applied view `version`, and the
 * owner answers "refresh required" with its own version when it does not own the grain.
 */
export interface DirectoryPeer {
  lookup(
    owner: SiloAddress,
    version: number,
    grainId: GrainId,
  ): Promise<DirectoryResult<GrainAddress | undefined>>;
  register(
    owner: SiloAddress,
    version: number,
    addr: GrainAddress,
    previous?: GrainAddress,
  ): Promise<DirectoryResult<GrainAddress>>;
  unregister(
    owner: SiloAddress,
    version: number,
    addr: GrainAddress,
  ): Promise<DirectoryResult<undefined>>;
}
