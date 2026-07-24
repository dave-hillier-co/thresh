import { it } from "vitest";

/**
 * Parity-gap tags: each names a feature a ported-but-unfinished test needs
 * that this framework does not have yet. The parity scorecard rolls skipped
 * tests up by tag, so the taxonomy doubles as the parity backlog; a tag with
 * no test currently using it is kept only while it still names a plausible
 * future gap, and is pruned once its feature ships or no port is expected to
 * need it.
 */
export type GapTag =
  | "GAP-STATELESS-WORKER" // StatelessWorker placement parsed but not enforced
  | "GAP-READONLY-ENFORCEMENT" // @readOnly is advisory by default; the opt-in dev-mode guard (ActivationData.readOnlyStateGuard) now covers persistent, reducer, durable-journaling and transactional state facets, but is off unless a silo opts in
  | "GAP-OBSERVERS" // no grain observers / client object references
  | "GAP-MGMT-GRAIN" // `IManagementGrain` now covers getHosts/getDetailedHosts/getSimpleGrainStatistics/getDetailedGrainStatistics/getActivationAddress/getGrainActivationCount/forceActivationCollection/sendControlCommandToProvider; kept for any upstream member still missing (e.g. GetRuntimeStatistics, ForceGarbageCollection)
  | "GAP-SERIALIZER-POLYMORPHISM" // serializer lacks polymorphism/cycles
  | "GAP-CLIENT-SILO-SEPARATION" // no separate client process distinct from a silo in the harness
  | "GAP-GRAIN-DIRECTORY-API" // internal directory not exposed via a pluggable IGrainDirectory API
  | "GAP-LOAD-SHEDDING" // overload detector / gateway load shedding exist; no CPU-aware placement scoring (placement never excludes an overloaded/busy silo)
  | "GAP-REBALANCER-CONTROL" // suspend/resume/report-listener control exists (SiloHost); kept for any upstream IActivationRebalancer surface still missing
  | "GAP-REQUEST-CONTEXT" // RequestContext-driven placement hints (IPlacementDirector.PlacementHintKey) now exist; kept for other client-side RequestContext scaffolding still missing (e.g. a grain-side fail-dehydrate injection knob a client would drive via RequestContext)
  | "GAP-STREAM-PROVIDER-CONFIG" // no config-load-failure distinction / typed stream-provider errors
  | "GAP-TRACING"; // no ActivitySource/span instrumentation on activation/call paths

type TestBody = () => void | Promise<void>;

/**
 * A vitest test ported 1:1 from the Orleans functional test suite. `id` is the
 * fully-qualified upstream xunit test id (namespace.class.method) at the pinned
 * Orleans tag — it becomes the vitest test name verbatim, so reporter output
 * and the parity scorecard both trace straight back to the original test.
 */
export function orleansTest(id: string, body: TestBody, timeoutMs?: number): void {
  it(id, body, timeoutMs);
}

/**
 * An upstream test whose feature does not exist here yet: registered as a skip,
 * prefixed with its machine-readable gap tag.
 */
orleansTest.gap = (tag: GapTag, id: string, body?: TestBody): void => {
  it.skip(`[${tag}] ${id}`, body ?? (() => undefined));
};

/**
 * An upstream `[Theory]`: one upstream id, several cases. Case labels append
 * after " :: " so the scorecard still counts a single upstream test.
 */
orleansTest.each = <T>(cases: readonly T[]) => {
  return (id: string, body: (item: T) => void | Promise<void>): void => {
    for (const [index, item] of cases.entries()) {
      it(`${id} :: case ${index}`, () => body(item));
    }
  };
};

/**
 * An upstream test deliberately not ported (e.g. exercises a .NET-specific
 * mechanism, or is skipped upstream). Registers nothing at runtime; the call
 * exists so the scorecard can account for every test in an in-scope file.
 */
orleansTest.excluded = (_reason: string, _id: string): void => undefined;
