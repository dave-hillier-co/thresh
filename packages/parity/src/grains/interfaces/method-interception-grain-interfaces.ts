// Ported from dotnet/orleans test/Grains/TestGrainInterfaces/IMethodInterceptionGrain.cs @ v10.1.0 (MIT).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

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
export interface IMethodInterceptionGrain extends GrainKey<bigint> {
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
 * Upstream `IMethodInterceptionGrainObserver`: the observer-side mirror of
 * `IMethodInterceptionGrain`, hosted via `ClientNode.createObjectReference`
 * rather than a grain activation (`Observer_GrainCallFilter_Incoming_*`).
 */
export interface IMethodInterceptionGrainObserver extends GrainKey<bigint> {
  one(): Promise<string>;
  echo(someArg: string): Promise<string>;
  notIntercepted(): Promise<string>;
  sayHello(): Promise<string>;
  doThrow(): Promise<string>;
  filterThrows(): Promise<void>;
  systemWideCallFilterMarker(): Promise<void>;
}

export const IMethodInterceptionGrainObserver =
  defineGrainInterface<IMethodInterceptionGrainObserver>(
    "UnitTests.GrainInterfaces.IMethodInterceptionGrainObserver",
  );

/**
 * Upstream `IOutgoingMethodInterceptionGrain`. `EchoViaOtherGrain` needs a
 * grain-to-grain outgoing call distinct from a client-issued one — exercised
 * via `ClientNode` (its own `outgoingCallFilters`, separate from the silo's),
 * not `TestCluster.getGrain` (which shares the silo's single `GrainFactory`
 * with every grain-to-grain call).
 */
export interface IOutgoingMethodInterceptionGrain extends GrainKey<bigint> {
  throwIfGreaterThanZero(value: number): Promise<string>;
  /** Upstream `EchoViaOtherGrain`: calls `otherGrain.Echo(message)`, wraps the result. */
  echoViaOtherGrain(
    otherGrain: IMethodInterceptionGrain,
    message: string,
  ): Promise<Record<string, unknown>>;
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
export interface IGrainCallFilterTestGrain extends GrainKey<bigint> {
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

/**
 * Upstream `IGrainCallFilterTestGrainObserver`: the observer-side mirror of
 * `IGrainCallFilterTestGrain`, hosted via `ClientNode.createObjectReference`
 * rather than a grain activation.
 */
export interface IGrainCallFilterTestGrainObserver extends GrainKey<bigint> {
  throwIfGreaterThanZero(value: number): Promise<string>;
  sumSet(numbers: readonly number[]): Promise<number>;
  systemWideCallFilterMarker(): Promise<void>;
  grainSpecificCallFilterMarker(): Promise<void>;
  getRequestContext(): Promise<string | undefined>;
}

export const IGrainCallFilterTestGrainObserver =
  defineGrainInterface<IGrainCallFilterTestGrainObserver>(
    "UnitTests.GrainInterfaces.IGrainCallFilterTestGrainObserver",
  );

/**
 * Upstream `IMyGrainExtension`: a `GrainExtension` cast onto an
 * `IMethodInterceptionGrain` reference (`GrainCallFilter_GrainExtension`),
 * proving filters wrap extension calls too. Upstream folds together
 * `IMyRegularInterface`/`IMyOtherInterface` — two interfaces the extension
 * implements explicitly with different `GetExtensionValue` bodies — to prove
 * interface-qua-`MethodInfo` resolution; there is no analogous explicit
 * interface implementation in TS (a single object exposes one method per
 * name), so this is flattened to the one `getExtensionValue` the observable
 * test behaviour (the silo-wide filter negating `setExtensionValue`'s
 * argument) actually needs.
 */
export interface IMyGrainExtension extends GrainKey<bigint> {
  setExtensionValue(value: number): Promise<void>;
  getExtensionValue(): Promise<number>;
}

export const IMyGrainExtension = defineGrainInterface<IMyGrainExtension>(
  "UnitTests.GrainInterfaces.IMyGrainExtension",
  { extension: true },
);
