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
  | "GAP-GENERIC-GRAINS" // open generic grain interfaces are unrepresentable
  | "GAP-ACTIVATION-REPARTITIONING" // no activation-repartitioning subsystem (communication-graph driven)
  | "GAP-BROADCAST-CHANNEL-CLIENT" // no client-side broadcast-channel writer/diagnostics/regex namespaces
  | "GAP-CALL-FILTER-CLIENT-LAYER" // no distinct client-issued vs grain-issued outgoing-filter layer
  | "GAP-CHANNEL-NAMESPACE-PREDICATE" // no IChannelNamespacePredicate provider abstraction
  | "GAP-CLIENT-REQUEST-CONTEXT" // no client-side ambient RequestContext outside a grain turn
  | "GAP-CLIENT-SILO-SEPARATION" // no separate client process distinct from a silo in the harness
  | "GAP-COMPOUND-KEY" // no compound key kind (guid/integer primary + string extension)
  | "GAP-DURABLE-COLLECTION-API" // DurableQueue/Set/List miss parts of Orleans' API surface
  | "GAP-EVENT-SOURCING" // no JournaledGrain<TState,TEvent>/log-consistency-provider mechanism
  | "GAP-GRAIN-DIRECTORY-API" // internal directory not exposed via a pluggable IGrainDirectory API
  | "GAP-GRAIN-REF-CAST" // no AsReference<T>() runtime re-typing of grain references
  | "GAP-GRAIN-SERVICE" // no per-silo grain-service registration/lifecycle mechanism
  | "GAP-JOB-SHARD-MANAGER-API" // no explicit JobShardManager/shard-object API (implicit time-bucketing)
  | "GAP-LOAD-SHEDDING" // no overload detector / load shedding / CPU-aware placement scoring
  | "GAP-PLACEMENT-FILTER-DIRECTORS" // no DI-based custom placement-filter directors
  | "GAP-PLACEMENT-INTROSPECTION" // a grain cannot learn its own hosting silo address
  | "GAP-REBALANCER-CONTROL" // no rebalancer suspend/resume/report-listener control API
  | "GAP-REQUEST-CONTEXT" // ambient RequestContext not exposed on @tsva/core's public surface
  | "GAP-SERVICE-ID" // no ServiceId concept surviving silo restarts
  | "GAP-STORAGE-FACET" // no third-party facet extensibility (named DI-resolved storage facets)
  | "GAP-STREAM-BATCHING" // no OnNextBatchAsync / batch-send stream API
  | "GAP-STREAM-CACHE-DIAGNOSTICS" // no pulling-agent cache/eviction model or diagnostic observer
  | "GAP-STREAM-FILTER" // no IStreamFilter server-side delivery filtering
  | "GAP-STREAM-GENERATOR-ADAPTER" // no synthetic-generator queue-adapter stream provider
  | "GAP-STREAM-IMPLICIT-MEMORY" // implicit subscriptions not delivered by the memory stream provider
  | "GAP-STREAM-PROVIDER-CONFIG" // no config-load-failure distinction / typed stream-provider errors
  | "GAP-STREAM-PROVIDER-WIRING" // streams not wired into TestCluster / no client.getStreamProvider
  | "GAP-STREAM-SUBSCRIPTION-MANAGER" // no administrative IStreamSubscriptionManager equivalent
  | "GAP-TIMER-VALIDATION" // GrainTimer.change() lacks due-time/period range validation
  | "GAP-TRACING" // no ActivitySource/span instrumentation on activation/call paths
  | "GAP-TRANSACTION-CONSISTENCY-HARNESS" // no randomized workload + serializable-history checker
  | "GAP-TRANSACTION-CONTEXT-INTROSPECTION" // no public API to read the ambient transaction id
  | "GAP-TRANSACTION-EXCEPTION-TYPES" // only a generic TransactionAbortedError, no typed hierarchy
  | "GAP-TRANSACTION-EXCLUSIVE-LOCK" // no shared-vs-exclusive lock distinction on transactional state
  | "GAP-TRANSACTION-OVERLOAD-DETECTOR" // no transaction-rate load shedding
  // Confirmed framework defects exposed by ported tests: fix the bug, then
  // un-gap the test. Each has a todo.md entry under "Bugs found by the parity suite".
  | "GAP-BUG-LOCAL-CALL-UNDEFINED"; // local calls return undefined where remote calls yield null

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
