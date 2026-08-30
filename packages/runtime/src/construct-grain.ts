import type { Grain } from "@thresh/core/grain";
import type { GrainClass } from "@thresh/core/grain-class";

/**
 * Construct a registered grain class with `new ctor()` — the default
 * activation path, and the single place in the codebase that asserts a
 * registered `GrainClass` is in fact zero-argument and concrete.
 *
 * `GrainClass` is `abstract new (...args: never[]) => Grain` so that a grain
 * whose constructor takes an options bag can be registered without a cast; the
 * price is that TypeScript will not call the constructor through that type.
 * Registration does not check arity either, and deliberately so: a grain with
 * required constructor parameters is legal to register, it simply has to be
 * built by a `GrainActivator` (`SiloBuilder.useGrainActivator`) — the seam that
 * exists precisely because Thresh has no constructor DI. This assertion
 * therefore covers the fall-through case only, and is exported so an
 * activator's own default branch can reuse it instead of minting its own cast.
 */
export function constructGrain(ctor: GrainClass): Grain {
  return new (ctor as new () => Grain)();
}
