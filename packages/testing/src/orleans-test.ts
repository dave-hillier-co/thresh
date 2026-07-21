import { it } from "vitest";

/**
 * Parity-gap tags: each names a feature the ported test needs that this
 * framework does not have yet. Every tag has a matching entry in `todo.md`;
 * the parity scorecard rolls skipped tests up by tag, so the taxonomy doubles
 * as the parity backlog.
 */
export type GapTag =
  // Missing features (each maps to a todo.md parity-gap item).
  | "GAP-STATELESS-WORKER" // StatelessWorker placement parsed but not enforced
  | "GAP-READONLY-ENFORCEMENT" // @readOnly is advisory; mutations are not rejected
  | "GAP-OBSERVERS" // no grain observers / client object references
  | "GAP-MGMT-GRAIN" // no management grain / silo-control system targets
  | "GAP-SERIALIZER-POLYMORPHISM" // serializer lacks polymorphism/cycles
  | "GAP-STREAM-FAILURE-HANDLER" // streams lack IStreamFailureHandler
  | "GAP-ACTIVATION-REPARTITIONING" // no activation-repartitioning subsystem (communication-graph driven)
  | "GAP-CLIENT-SILO-SEPARATION" // no separate client process distinct from a silo in the harness
  | "GAP-GRAIN-DIRECTORY-API" // internal directory not exposed via a pluggable IGrainDirectory API
  | "GAP-GRAIN-SERVICE" // no per-silo grain-service registration/lifecycle mechanism
  | "GAP-LOAD-SHEDDING" // overload detector / gateway load shedding exist; no CPU-aware placement scoring (placement never excludes an overloaded/busy silo)
  | "GAP-REBALANCER-CONTROL" // no rebalancer suspend/resume/report-listener control API
  | "GAP-REQUEST-CONTEXT" // RequestContext-driven placement hints (IPlacementDirector.PlacementHintKey) now exist; kept for other client-side RequestContext scaffolding still missing (e.g. a grain-side fail-dehydrate injection knob a client would drive via RequestContext)
  | "GAP-STREAM-FILTER" // no IStreamFilter server-side delivery filtering
  | "GAP-STREAM-PROVIDER-CONFIG" // no config-load-failure distinction / typed stream-provider errors
  | "GAP-STREAM-PROVIDER-WIRING" // streams not wired into TestCluster / no client.getStreamProvider
  | "GAP-TIMER-VALIDATION" // GrainTimer.change() lacks due-time/period range validation
  | "GAP-TRACING" // no ActivitySource/span instrumentation on activation/call paths
  | "GAP-TRANSACTION-CONSISTENCY-HARNESS" // no randomized workload + serializable-history checker
  | "GAP-TRANSACTION-CONTEXT-INTROSPECTION" // no public API to read the ambient transaction id
  | "GAP-TRANSACTION-EXCEPTION-TYPES" // only a generic TransactionAbortedError, no typed hierarchy
  | "GAP-TRANSACTION-EXCLUSIVE-LOCK" // no shared-vs-exclusive lock distinction on transactional state
  | "GAP-TRANSACTION-OVERLOAD-DETECTOR"; // no transaction-rate load shedding

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
