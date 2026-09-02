import { defineGrainInterface } from "./grain-interface";
import type { GrainKey } from "./key-kinds";

/**
 * Orleans' `ISiloControl.Ping()` (the health-check corner of
 * `ISiloControl`), pared down to exactly the no-op self-probe surface this
 * port needs: docs/design-notes-parity-gaps.md item 9, option A. A silo
 * calls `ping()` on ITSELF — `getGrain(ISiloProbe, <this silo's own ring
 * key>)` — with `preferLocal` placement (see
 * `createSiloProbeGrainType`/`ClusterNode`) so the call activates and
 * schedules a real turn on the real dispatcher instead of being answered by
 * a shortcut, and `SelfProbeWorker` (`@thresh/hosting`) times the round trip
 * on a cadence to detect a wedged turn scheduler — a hung silo that would
 * otherwise still answer k8s' readiness probe with 200.
 *
 * Keyed by string (the calling silo's own `SiloAddress.ringKey`) rather than
 * `IManagementGrain`'s single well-known `0n`, since every silo hosts its
 * own independent activation of this grain — there is deliberately no
 * cross-silo peer probing (see the "Constraint" note in the design doc: K8s
 * stays the sole membership authority).
 */
export interface ISiloProbe extends GrainKey<string> {
  /** No-op: answering at all (before the probe's own deadline) is the whole signal. */
  ping(): Promise<void>;
}

export const ISiloProbe = defineGrainInterface<ISiloProbe>("Thresh.Runtime.ISiloProbe");
