// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ISimplePersistentGrain.cs @ v10.1.0 (MIT).
// Upstream also declares GetRequestContext/SetRequestContext (Orleans
// `RequestContext` ambient call-context propagation); no ported test in this
// suite exercises them, so they are omitted here.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { ISimpleGrain } from "@tsva/parity/grains/interfaces/simple-grain-interfaces";

// Upstream overloads SetA(a)/SetA(a, deactivate); TS interfaces cannot overload
// across the wire (dispatch is by method name), so the two-arg form is
// setADeactivating, matching the getAxBArgs convention on ISimpleGrain.
export interface ISimplePersistentGrain extends ISimpleGrain {
  setADeactivating(a: number, deactivate: boolean): Promise<void>;
  getVersion(): Promise<string>;
}

export const ISimplePersistentGrain = defineGrainInterface<ISimplePersistentGrain>(
  "UnitTests.GrainInterfaces.ISimplePersistentGrain",
);
