# todo

Outstanding work only. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)).

## Parity gaps

- [ ] **Cancellation tokens & per-call deadlines end-to-end** — thread a `CancellationToken`
      through the dispatcher, turn scheduler, persistence/storage providers and the deactivation
      path so hung methods, slow downstreams and shutdown can be interrupted. Prerequisite for the
      `onDeactivate` timeout, TM cancel and stream redelivery backoff.
- [ ] **Scheduler back-pressure, stuck-turn detection & `onDeactivate` timeout** — bounded
      per-activation queue with soft/hard limits (`MaxEnqueuedRequestsSoftLimit`/`HardLimit`),
      `MaxRequestProcessingTime` stuck-turn detection, and an enforced `DeactivationTimeout` that
      force-invalidates the activation so one bad grain cannot OOM or block silo shutdown.
- [ ] **Runtime instrumentation breadth** — wire the OTel meters already exposed by
      `@tsva/observability` into the catalog, directory, persistence, messaging, reminders, streams
      and durable-jobs runtimes, and add `exception.type` / `exception.message` /
      `exception.stacktrace` attributes to spans.
- [ ] **Versioned, registry-based serializer** — replace the 6-tag value codec with a versioned
      serializer that supports surrogate types, polymorphism resolution, `Map`/`Set`,
      circular-reference handling and per-field version tags so rolling upgrades and event-sourced
      payloads are safe.
- [ ] **Stream `IStreamFailureHandler` wiring & producer registration** — forward the configurable
      failure handler from `RedisPullingStreamProvider` to the agent, and persist poison events to
      a durable failure store (Orleans `AzureTableStorageStreamFailureHandler` equivalent). Add
      explicit producer registration.
- [ ] **Transaction TM confirmation-worker keepalive** — pair with the lock-acquisition deadline so
      the TM periodically pings remote participants to resolve in-doubt prepared records after a TM
      crash, rather than relying on one-shot recovery at next activation.
- [ ] **Grain observers / typed client callbacks** — `CreateObjectReference<T>()` surface for
      server-to-client push, including W3C `traceparent` propagation back to the client.
- [ ] **`IGrainExtension` mechanism** — minimal extension surface so management/system-target style
      APIs, cancellation tokens and per-grain push surfaces can be expressed.
- [ ] **`StatelessWorker` placement enforcement** — honor the option in the catalog/dispatcher so
      multiple activations per key can be created up to `maxLocalWorkers`, with placement-local
      delivery.
- [ ] **`@readOnly` runtime check** — at least a dev-mode mutation guard that detects state writes
      from a `@readOnly` call and surfaces the violation.
- [ ] **Directory handoff ACK & cleanup** — ACK-delete loop on snapshot handoff so retained
      snapshots are expired when a successor crashes pre-pull; add retry-with-backoff on recovery
      pulls and gate concurrent registers on a `recoveryMembershipVersion`.

## Orleans test-suite port (parity suite)

The functional test suites of Orleans `v10.1.0` are ported 1:1 into `packages/parity` (see its
README for conventions); `pnpm parity:scorecard [--run]` reports the current standing. All ten
in-scope upstream projects are enumerated — every upstream test is ported (passing), gap-tagged
(skipped pending a feature below), or excluded with a reason. Gap-tagged skips (`GAP-*` in
`@tsva/testing/orleans-test`) map onto the backlog below; implementing a feature un-gaps its
tests, which is how the scorecard measures parity progress. Notes: Orleans has no separate
Reminders test project at v10.1.0 (reminder coverage lives in DefaultCluster/Runtime suites),
and upstream itself skips its golden-path transaction runner (dotnet/orleans#9553), so
transaction behaviour here remains covered by `packages/hosting`'s transactions-cluster tests.

- [ ] **Migrate the ad-hoc `buildCluster` helpers** in `packages/runtime`/`hosting` tests onto
      `@tsva/testing`'s `TestCluster` (optional follow-up).

### Gap backlog from the port (tags in `@tsva/testing/orleans-test`)

Each `GAP-*` tag below skips ported tests; implementing the feature un-gaps them (the scorecard
shows per-tag counts). One-line definitions live on the `GapTag` union. Grouped by area:

- [ ] **Grain identity & references** — `GAP-COMPOUND-KEY` (guid+string compound keys),
      `GAP-GRAIN-REF-CAST` (`AsReference<T>()` re-typing), `GAP-GENERIC-GRAINS` (closed-generic
      grain interface story).
- [ ] **Activation & lifecycle** — `GAP-DEACTIVATE-DURING-ACTIVATION`, `GAP-LIFECYCLE-SUBJECT`,
      `GAP-GRAIN-ACTIVATOR`, `GAP-STARTUP-TASK`, `GAP-GRAIN-SERVICE`, `GAP-STORAGE-FACET`,
      `GAP-MIGRATE-FROM-DEACTIVATE`.
- [ ] **Request context** — `GAP-REQUEST-CONTEXT` (expose on `@tsva/core` public surface),
      `GAP-CLIENT-REQUEST-CONTEXT`, `GAP-CALL-FILTER-CLIENT-LAYER`, `GAP-CLIENT-SILO-SEPARATION`.
- [ ] **Timers** — `GAP-TIMER-INTERLEAVE` (per-timer interleave option), `GAP-TIMER-VALIDATION`.
- [ ] **Placement & rebalancing** — `GAP-PLACEMENT-FILTER-DIRECTORS`, `GAP-PLACEMENT-INTROSPECTION`,
      `GAP-RESOURCE-OPTIMIZED-OPTIONS`, `GAP-REBALANCER-CONTROL`, `GAP-ACTIVATION-REPARTITIONING`,
      `GAP-LOAD-SHEDDING`, `GAP-SILO-ROLE-CONFIG`.
- [ ] **Streams** — `GAP-STREAM-IMPLICIT-MEMORY` (implicit subscriptions on the memory provider),
      `GAP-STREAM-BATCHING`, `GAP-STREAM-FILTER`, `GAP-STREAM-SUBSCRIPTION-MANAGER`,
      `GAP-STREAM-PROVIDER-WIRING` (TestCluster/client surface), `GAP-STREAM-PROVIDER-CONFIG`,
      `GAP-STREAM-CACHE-DIAGNOSTICS`, `GAP-STREAM-GENERATOR-ADAPTER`,
      `GAP-BROADCAST-CHANNEL-CLIENT`, `GAP-CHANNEL-NAMESPACE-PREDICATE`.
- [ ] **Transactions** — `GAP-TRANSACTION-EXCEPTION-TYPES` (typed abort hierarchy),
      `GAP-TRANSACTION-CONTEXT-INTROSPECTION`, `GAP-TRANSACTION-EXCLUSIVE-LOCK`,
      `GAP-TRANSACTION-OVERLOAD-DETECTOR`, `GAP-TRANSACTIONS-OPT-OUT`,
      `GAP-TRANSACTION-CONSISTENCY-HARNESS` (randomized workload + serializability checker).
- [ ] **Journaling & event sourcing** — `GAP-EVENT-SOURCING` (`JournaledGrain` equivalent),
      `GAP-DURABLE-COLLECTION-API`, `GAP-STATE-MACHINE-RETIREMENT`.
- [ ] **Durable jobs** — `GAP-JOB-SHARD-MANAGER-API`, `GAP-CLAIM-BUDGET-RAMPUP`.
- [ ] **Misc primitives** — `GAP-LEASE-PROVIDER`, `GAP-ASYNC-SERIAL-EXECUTOR`, `GAP-RETRY-EXECUTOR`,
      `GAP-GRAIN-DIRECTORY-API`, `GAP-SERVICE-ID`, `GAP-TRACING`.

### Bugs found by the parity suite (`GAP-BUG-*`, fix then un-gap the tests)

- [ ] **`GAP-BUG-CALL-FILTER-REQUEST-CONTEXT`** — header writes by incoming filters never reach
      the ambient request context seen by the grain method body. `packages/runtime/src/activation.ts`.
- [ ] **`GAP-BUG-LOCAL-CALL-UNDEFINED`** — `LocalDispatcher` returns `undefined` unchanged while
      cross-silo calls serialize it to `null`, so a value's shape depends on placement.
- [ ] **`GAP-BUG-DURABLE-JOBS-QUEUE`** — `InMemoryJobQueue.retryLater` lacks an existence guard;
      `ShardExecutor` slow-start ramp is not time-gated. `packages/durable-jobs/src/`.
- [ ] **TestCluster teardown flakiness** — `dispose()` on a 2-silo cluster intermittently races
      (seen in `grain-activate-deactivate-tests`); harden shutdown ordering in
      `packages/testing/src/test-cluster.ts`.
- [ ] **Migration rehydration failure leaves a stale directory entry** — when a migrated
      activation's `onRehydrate` throws on the target silo, the directory keeps pointing at the
      invalid activation (found by the ported `FailRehydrationTest` scenario). `packages/runtime`.
- [ ] **Durable jobs cross-silo delivery** — `scheduleJob()` only claims on the scheduling silo;
      with a shared store on a multi-silo cluster, jobs scheduled for grains owned by another
      silo may never start. `packages/hosting` durable-jobs wiring (verify then fix).

## Parity follow-ups (from in-flight slices)

- [ ] **Reminders — surface `getReminder`/`getReminders` through `GrainRuntime`** so grains can
      introspect their own reminders (Orleans `IReminderRegistry.GetReminder` parity); currently
      exposed only at the registry level.
- [ ] **Reminders — plumb `ReminderOptions` (including `minimumPeriod`) through the silo builder**
      so hosts can configure the minimum without constructing `LocalReminderService` by hand.
- [ ] **Directory — apply silo-liveness gate to `register`** (Orleans `RegisterCore` treats an
      existing entry whose silo is dead as overwritable).
- [ ] **Client — per-attempt deadline** so the cumulative `callTimeoutMs` accounts for time spent
      in gateway-failover backoff.
- [ ] **Durable jobs — in-grain `RunId` dedup** on the delivery path
      (`cluster-node.deliverDurableJob`) mirroring Orleans
      `DurableJobReceiverExtension._runningJobs`, so concurrent re-deliveries of an in-flight RunId
      can be coalesced in the receiving grain in addition to the executor-side post-completion
      dedup.

## Beyond parity

- [ ] **Browser state replication & browser-hosted grains** — read-only live read-views first;
      implementation pending.

## Deferred

- [ ] Additional stream backings behind the existing interfaces (Redis is the default). Postgres
      grain-storage and reminder providers already ship.
