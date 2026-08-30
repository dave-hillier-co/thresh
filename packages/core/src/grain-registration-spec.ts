import type { GrainClass } from "./grain-class";
import type { GrainInterface } from "./grain-interface";

/**
 * One entry in a grain-registration list: an implementation class and the
 * interfaces that address it. TypeScript interfaces are erased, so a silo (and
 * a cluster client, which hosts no activations but still has to resolve
 * `getGrain`) must be told the mapping explicitly.
 *
 * `readonly` throughout so a consumer can declare its registration list as a
 * shared module constant and pass the same array to every silo and client,
 * rather than defensively copying it at each call site.
 */
export interface GrainRegistrationSpec {
  readonly ctor: GrainClass;
  readonly interfaces: readonly GrainInterface<unknown>[];
}
