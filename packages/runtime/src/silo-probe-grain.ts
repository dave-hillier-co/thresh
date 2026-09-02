import { Grain } from "@thresh/core/grain";
import { setGrainOptions } from "@thresh/core/grain-metadata";
import { ISiloProbe } from "@thresh/core/silo-probe-grain";
import type { GrainType } from "@thresh/core/grain-type";

export { ISiloProbe };

/** The grain type the built-in self-probe grain registers under on every silo. */
export const SILO_PROBE_GRAIN_TYPE: GrainType = "Thresh.Runtime.SiloProbeGrain";

/**
 * Builds the built-in self-probe grain class, mirroring
 * `createManagementGrainType`: a fresh class (rather than one shared
 * instance) because the silo constructs it through the default `new ctor()`
 * path, so `setGrainOptions` is called directly instead of the `@grain()`
 * decorator. Unlike the management grain this one closes over nothing — the
 * whole point is that `ping()` is a true no-op, so a timeout means the
 * DISPATCH pipeline (queueing, admission, the turn scheduler) is wedged, not
 * that some application-level dependency it calls out to is slow.
 *
 * Registered with `placement: "preferLocal"` (`ClusterNode`'s constructor):
 * every silo activates its OWN instance rather than routing to whichever
 * silo placement would otherwise pick, so `getGrain(ISiloProbe,
 * local.ringKey).ping()` genuinely exercises the calling silo's own
 * dispatcher — the self-probe would be meaningless if it could silently
 * land on a healthy peer instead.
 */
export function createSiloProbeGrainType(): new () => Grain {
  class SiloProbeGrain extends Grain implements ISiloProbe {
    async ping(): Promise<void> {
      // Intentionally empty: answering at all, inside the caller's deadline,
      // is the entire signal (Orleans `ISiloControl.Ping`).
    }
  }
  // `immovable: "any"`: the probe is only meaningful on the silo it probes —
  // a rebalancer or repartitioner moving it to a peer would leave every later
  // `ping()` exercising the WRONG silo's dispatcher (preferLocal only applies
  // at first activation, not to an already-active grain elsewhere).
  setGrainOptions(SiloProbeGrain, SILO_PROBE_GRAIN_TYPE, {
    placement: "preferLocal",
    immovable: "any",
  });
  return SiloProbeGrain;
}
