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
 * Served by the node's local system-activation path (`ClusterNode`'s
 * `isLocalSystemType`): a call to it is never placed, never looked up in or
 * registered with the directory, and is always answered by an activation on
 * the silo that made it — created through the catalog and run through its
 * scheduler like any call's turn — so `getGrain(ISiloProbe,
 * local.ringKey).ping()` genuinely exercises the calling silo's own
 * dispatcher, including on a silo that is not a placement candidate (a
 * joining or draining one). The self-probe would be meaningless if it could
 * land on a healthy peer instead. `preferLocal` placement is kept on the
 * class for a type registered without that path.
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
