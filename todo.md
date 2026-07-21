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
- [ ] **Grain observers / typed client callbacks** — `CreateObjectReference<T>()`/
      `deleteObjectReference` (`ClientNode`, and now `GrainFactoryAccess` for silo startup tasks
      via an embedded `ClientNode` — `SiloBuilder.build()`, requires `useInProcessTransport`) exist
      for server-to-client push; still missing: W3C `traceparent` propagation back to the client.
- [x] **`StatelessWorker` placement enforcement** — the catalog now scales a stateless-worker grain
      id to multiple local activations on demand (up to `maxLocalWorkers`), routed synchronously
      (no directory) from `DistributedDispatcher`/`LocalDispatcher`; ordinary grains keep exactly
      one activation per id. `IManagementGrain.getGrainActivationCount`/`forceActivationCollection`
      added to observe/force-collect it. GAP-STATELESS-WORKER's remaining 3 skips are unrelated
      (stream-provider wiring, 2 placement-test cases) — see their own gap comments.
- [x] **`IManagementGrain.getDetailedGrainStatistics`** — one entry per live activation
      (grain type, silo, grain id) amalgamated across the active cluster, alongside the existing
      `getHosts`/`getDetailedHosts`/`getSimpleGrainStatistics`/`getActivationAddress`/
      `getGrainActivationCount`/`forceActivationCollection`/`sendControlCommandToProvider` surface
      (GAP-MGMT-GRAIN closed — the tag stays defined for any future upstream `IManagementGrain`
      member this port doesn't cover yet, e.g. `GetRuntimeStatistics`/`ForceGarbageCollection`).
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

- [x] **Activation & lifecycle** — `GAP-GRAIN-SERVICE` closed: per-silo `GrainService` base class +
      `GrainServiceRegistry`, reached from a grain via `GrainRuntime.getGrainService` (built on the
      same seams as grain extensions). `GAP-STORAGE-FACET` was already stale (no longer a `GapTag`).
- [ ] **Request context** — `GAP-CLIENT-REQUEST-CONTEXT`, `GAP-CALL-FILTER-CLIENT-LAYER`.
      `GAP-REQUEST-CONTEXT` closed: `ActivationData.requestMigration` now captures the whole
      ambient `RequestContext` bag (not just the placement hint) and `dehydrate` restores it around
      `onDehydrate`, so a migration participant can read caller-set ambient data (e.g.
      `FailDehydrationTest`'s `fail_dehydrate` flag); a participant that throws is swallowed
      (Orleans: log-and-continue) rather than aborting the whole migration.
      `GAP-CLIENT-SILO-SEPARATION` closed: `ClientNode.isActive` always answers `false` (a client
      never hosts grain activations), mirroring `SiloHost.isActive`'s silo-side check.
- [ ] **Timers** — `GAP-TIMER-VALIDATION` (callback-initiated Change/dispose portions).
- [ ] **Placement & rebalancing** — `GAP-ACTIVATION-REPARTITIONING`, `GAP-LOAD-SHEDDING`.
      `GAP-REBALANCER-CONTROL` closed: `ActivationRebalancerWorker.report()`'s `host` is now the
      currently elected leader's ring key (computed identically from the shared membership view on
      every silo's own worker instance, not the instance's own address), so any silo's
      `SiloHost.getRebalancingReport()` agrees on the same elected host — no system-target routing
      layer needed since leader election here is already deterministic from membership alone.
- [ ] **Streams** — the memory stream provider now delivers to implicit subscribers, supports
      batch publish/delivery, and has an administrative `StreamSubscriptionManager`
      (`GAP-STREAM-IMPLICIT-MEMORY`/`GAP-STREAM-BATCHING`/`GAP-STREAM-SUBSCRIPTION-MANAGER` closed).
      Remaining: `GAP-STREAM-FILTER`,
      `GAP-STREAM-PROVIDER-WIRING` (stream delivery — `StreamConsumerInterface`/
      `BroadcastConsumerInterface` — bypasses the incoming-call-filter pipeline in
      `Activation.callMethod`; `TestCluster`/`SiloHost.getStreamProvider` now exist, so the
      remaining gap is filter coverage, not provider access), `GAP-STREAM-PROVIDER-CONFIG`.
      `GAP-STREAM-CACHE-DIAGNOSTICS` closed: `StreamingDiagnosticObserver`
      (`@tsva/parity/support/streaming-diagnostics`) plus `MemoryStreamProvider.fanOut` now
      dispatching to every implicit/admin subscriber concurrently instead of one at a time, so a
      slow-activating subscriber no longer stalls a fast one sharing the same stream. `GAP-STREAM-GENERATOR-ADAPTER`
      closed: `RecoverableStreamDeliveryError`/`RecoverableCheckpoint`
      (`packages/streams/src/stream-recovery.ts`, `generated-stream-recovery-tests.test.ts`) — a
      per-subscription recoverable-cursor mechanism so a fault-injecting collector grain can
      deactivate and have `QueuePullingAgent` rewind the owning queue to its last checkpoint and
      redeliver, vs. a non-transient fault permanently skipping just the one event. `GAP-BROADCAST-CHANNEL-CLIENT`
      closed: `ClientNode.getBroadcastChannelProvider`, per-provider `fireAndForgetDelivery`,
      the `@tsva/core/broadcast-channel-diagnostics` event bus, and predicate-based subscriber
      matching wired into `ClusterNode.broadcastGrainTypes`.
- [ ] **`MemoryStreamProvider` is per-silo, not cluster-shared** — `SiloBuilder.useMemoryStreams`
      builds a fresh provider (and fresh admin subscription registry) on every silo that calls it;
      `TestCluster`'s other backends (storage, reminders, journals, jobs) are explicitly shared
      cluster-wide, streams are not. A producer grain and a consumer grain (implicit or
      administratively-subscribed) landing on different silos of a multi-silo cluster publish
      into, and register against, two independent provider instances that never see each other.
      Currently sidestepped in `packages/parity/src/streaming/memory-programmatic-subcribe-tests.test.ts`
      by pinning `initialSilos: 1`; fix properly by sharing named `MemoryStreamProvider` instances
      across a `TestCluster` the way `storage`/`reminderTable`/etc. already are.
- [x] **Transactions** — all `GAP-TRANSACTION-*` tags closed: typed abort/in-doubt exception
      hierarchy (`TransactionOrphanCallError`, `TransactionCascadingAbortError`,
      `TransactionInDoubtError`), ambient-transaction-id introspection
      (`GrainRuntime.getTransactionId()`), the transaction-rate overload detector
      (`@tsva/transactions/transaction-overload-detector`), and a randomized-workload +
      serializable-history checker (`packages/parity/src/transactions/consistency-harness.ts`).
      One case (`ExclusiveLockTransactionMemoryTests.ConcurrentReadThenWriteWithExclusiveLock_NoLockException`)
      stays `orleansTest.excluded`: it needs Orleans' non-killing exclusive-lock group-batch
      scheduling, which would mean redesigning `ReaderWriterLock`'s core wait-die conflict
      resolution — out of scope (see the test file's header comment).
- [x] **Journaling & event sourcing** — `GAP-EVENT-SOURCING` (`JournaledGrain` equivalent): done —
      `JournaledGrain<TState,TEvent>` (`packages/core/src/journaled-grain.ts`) plus the
      `LogViewAdaptor` provider (`packages/journaling/src/log-view-adaptor-impl.ts`,
      `journaled-grain-binder.ts`), built on the existing journal-storage substrate and wired into
      the silo builder's `stateBinder`. `CountersGrain`/`LogTestGrain`/`PersonGrain` ported as real
      journaled grains; `event-sourcing` parity suite now 21 ported / 0 gap.
- [x] **Grain directory API** — `GAP-GRAIN-DIRECTORY-API`: done — `ClusterNode.grainDirectory()` /
      `SiloHost.directory` expose the silo's real `DistributedGrainDirectory` (`@tsva/directory`,
      now a `@tsva/parity` dependency) as a public `GrainDirectory` (Orleans
      `GrainDirectoryResolver.DefaultGrainDirectory`); `grain-directory` parity suite now
      4 ported / 0 gap.
- [x] **`GAP-TRACING`** — closed (0 gaps remaining in the scorecard). Activation/deactivation/
      placement span taxonomy (`ActivateGrain`/`OnActivate`/`PlaceGrain`/`FilterPlacementCandidates`/
      `RegisterDirectoryEntry`/`StorageRead`/`StorageWrite`/`OnDeactivate`, plus migration's
      `Dehydrate`/`Rehydrate`) threaded through placement (`DistributedDispatcher.placeAndInvoke`,
      `ClusterNode.migrateActivation`), catalog/activation, directory registration, and storage —
      `@tsva/observability/activation-tracing`. Baggage propagation added to
      `setupTracePropagation()` (`CompositePropagator` of W3C trace-context + baggage). A handful of
      upstream cases stay `orleansTest.excluded` (not gapped) for genuine architectural reasons: no
      `Grain`-base-vs-plain-implementer (`IGrainBase`) distinction to hinge `OnActivate`/`OnDeactivate`
      span presence on (every grain here extends the same `Grain` base), no IAsyncEnumerable
      grain-method span story, no auto-deactivate-on-exception or `GrainContext.Deactivate(reason)`
      API, and no `ActivityIdFormat` concept (OTel is W3C-only).
>>>>>>> parity/tracing-span-taxonomy

### Bugs found by the parity suite (`GAP-BUG-*`, fix then un-gap the tests)

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
