// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IMethodInterceptionGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithIntegerKey } from "@tsva/core/key-kinds";

/**
 * Upstream `SayHello` lives on a separate `IMethodFromAnotherInterface` that
 * `IMethodInterceptionGrain` also extends; folded in directly since TS grain
 * interfaces are flattened for wire dispatch. `IncorrectResultType` is omitted:
 * it exists upstream to prove a filter that sets a mistyped `Result` causes an
 * `InvalidCastException` when the caller's generic `Task<string>` unwraps a
 * `Guid` — there is no analogous generic-result cast in JS's untyped runtime
 * values, so the test that exercises it is excluded rather than the method
 * being ported unused.
 */
export interface IMethodInterceptionGrain extends GrainWithIntegerKey {
  one(): Promise<string>;
  echo(someArg: string): Promise<string>;
  notIntercepted(): Promise<string>;
  sayHello(): Promise<string>;
  doThrow(): Promise<string>;
  filterThrows(): Promise<void>;
  systemWideCallFilterMarker(): Promise<void>;
}

export const IMethodInterceptionGrain = defineGrainInterface<IMethodInterceptionGrain>(
  "UnitTests.GrainInterfaces.IMethodInterceptionGrain",
);

/**
 * Upstream `IOutgoingMethodInterceptionGrain`; `EchoViaOtherGrain` is omitted
 * because exercising it needs a distinct client-vs-grain outgoing-filter layer
 * that this harness does not model (`TestCluster` has no separate client
 * process — see `GrainCallFilter_Outgoing_Test`'s gap in grain-call-filter-tests.test.ts).
 */
export interface IOutgoingMethodInterceptionGrain extends GrainWithIntegerKey {
  throwIfGreaterThanZero(value: number): Promise<string>;
}

export const IOutgoingMethodInterceptionGrain =
  defineGrainInterface<IOutgoingMethodInterceptionGrain>(
    "UnitTests.GrainInterfaces.IOutgoingMethodInterceptionGrain",
  );

/**
 * Upstream `IGrainCallFilterTestGrain`. `GetRequestContext` reads the ambient
 * RequestContext value that the silo-wide and grain-level incoming filters
 * progressively build up (via the now-exposed `GrainRuntime.getRequestContext`/
 * `setRequestContext`), proving a filter's header write reaches the method body.
 */
export interface IGrainCallFilterTestGrain extends GrainWithIntegerKey {
  throwIfGreaterThanZero(value: number): Promise<string>;
  // Upstream: `HashSet<int>`. A JS `Set` does not round-trip across a real
  // (cross-silo) call boundary here (it is not structurally cloneable the way
  // `TestCluster`'s serializer expects), so a plain array stands in — the
  // test still exercises a filter passing through a collection-typed argument.
  sumSet(numbers: readonly number[]): Promise<number>;
  systemWideCallFilterMarker(): Promise<void>;
  grainSpecificCallFilterMarker(): Promise<void>;
  /** Upstream `GetRequestContext`: reads back the `RequestContext` value filters wrote. */
  getRequestContext(): Promise<string | undefined>;
}

export const IGrainCallFilterTestGrain = defineGrainInterface<IGrainCallFilterTestGrain>(
  "UnitTests.GrainInterfaces.IGrainCallFilterTestGrain",
);
