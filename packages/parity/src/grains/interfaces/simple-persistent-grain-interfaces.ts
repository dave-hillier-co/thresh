// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/ISimplePersistentGrain.cs @ v10.1.0 (MIT).
// Upstream also declares SetRequestContext (Orleans `RequestContext` ambient
// call-context propagation, callee-to-caller direction); no ported test in
// this suite exercises that direction (`RequestContextCalleeToCallerFlow` is
// gapped upstream itself, `[Fact(Skip = ...)]`), so it is omitted here.
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { ISimpleGrain } from "@tsva/parity/grains/interfaces/simple-grain-interfaces";

// Upstream overloads SetA(a)/SetA(a, deactivate); TS interfaces cannot overload
// across the wire (dispatch is by method name), so the two-arg form is
// setADeactivating, matching the getAxBArgs convention on ISimpleGrain.
export interface ISimplePersistentGrain extends ISimpleGrain {
  setADeactivating(a: number, deactivate: boolean): Promise<void>;
  getVersion(): Promise<string>;
  /**
   * Reads the `"GrainInfo"` ambient `RequestContext` header a caller set
   * before this call (Orleans `GetRequestContext`). Values in this
   * framework's `RequestContext` are strings (there is no boxed-object wire
   * type), unlike upstream's `object`.
   */
  getRequestContext(): Promise<string | undefined>;
}

export const ISimplePersistentGrain = defineGrainInterface<ISimplePersistentGrain>(
  "UnitTests.GrainInterfaces.ISimplePersistentGrain",
);
