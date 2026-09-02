# EPICS

Status board for the **Orleans 10 parity** port (a TypeScript, reducer/functional-first virtual-actor
runtime). See [`todo.md`](todo.md) for outstanding work and [`docs/deviations.md`](docs/deviations.md)
for how the design differs from Orleans.

## ✅ Shipped

- **Core actor model** — grains, identity, `Proxy` references, turn scheduler, activation lifecycle,
  call-chain reentrancy.
- **Messaging & multi-silo** — WebSocket/in-process transport, message envelope, serializer,
  dispatcher, DHT directory + location cache with versioned lossless range handoff, static membership.
- **Placement** — random / prefer-local / activation-count / stateless-worker / silo-role /
  resource-optimized strategies, metadata placement filters, and version-aware placement.
- **Kubernetes hosting** — EndpointSlice-watch membership, health probes, graceful drain, the
  `createSilo()` builder, a self-probe that fails readiness when the silo's own dispatcher hangs
  (the `ISiloProbe` grain + `SelfProbeWorker`), and a real-cluster e2e.
- **Persistence** — `PersistentState` facet with etag concurrency; memory + Redis + Postgres providers.
- **Timers & reminders** — in-memory timers; durable reminders with hash-range ownership and
  rebalancing; memory + Redis + Postgres tables.
- **Event streams** — `StreamProvider`/subscriptions, pulling agents, ring-based queue ownership,
  durable cursors, at-least-once delivery, implicit subscriptions; memory + Redis Streams +
  Postgres + Kafka backings behind a shared backend-neutral provider core (see
  [`docs/stream-backings-postgres-kafka.md`](docs/stream-backings-postgres-kafka.md)).
- **Cross-grain ACID transactions** — `TransactionalState<T>` with timestamp-ordered wait-die
  locking, optimistic two-phase commit (TM elected from writers), durable storage, cross-silo
  participants, in-doubt recovery.
- **Durable journaling (`DurableGrain`)** — a per-grain `StateMachineManager` over
  `DurableValue`/`DurableDictionary`/`DurableList`/`DurableQueue`/`DurableSet` that journals each
  mutation to an append-only log
  and replays it on activation, with snapshot/compaction; a `JournalStorage` seam (memory + Redis),
  `useDurable*` hooks + `@durable*` decorators, and `useMemoryJournaling()`/`addRedisJournaling()`.
  Separate from the reducer snapshot facet and `PersistentState`.
- **Grain migration** — live activation move with state preserved (`IGrainMigrationParticipant`,
  `MigrateOnIdle`, directed placement).
- **Grain-interface versioning** — versions coexist for heterogeneous rolling upgrades, with
  version-aware placement.
- **Broadcast channels** — lightweight in-cluster pub/sub to implicit subscribers.
- **Grain call filters** — incoming, outgoing, and per-grain interception.
- **Custom-storage log consistency** — a `JournaledGrain` that also implements
  `CustomStorageInterface` owns its own log persistence (per-version rows, snapshots,
  compaction) and the binder routes it past the journal substrate, mirroring Orleans'
  `ICustomStorageInterface<TState, TDelta>` and its `CustomStorageAdaptor`.
- **Custom placement strategies** — named strategies registered with
  `addPlacementStrategy(name, strategy)` and selected by `{ placement: "custom", strategy }`,
  the whole-strategy counterpart of the existing placement-filter registry (Orleans
  `IPlacementDirector`).
- **External client** — gateway-routed client with gateway discovery + failover, verified in-process
  and over real WebSocket sockets.
- **Reducer & functional-first authoring** — `defineGrain` + hooks, snapshot reducers, dispatch
  grains. Every example grain is functional (one `@grain()` class kept on purpose as the living
  interop example). Hooks take no context argument (rules-of-hooks: synchronous, top of the factory
  only); a `defineGrain` definition **is** its own `GrainInterface`, with the message surface inferred
  from the factory's return type and `interfaceName` available to pin the wire name; key kinds are
  declared as a type argument (`defineGrainInterface<ICounter, "integer">`) instead of phantom marker
  interfaces; `registerGrain(def)` takes the `{ interfaces: [...] }` descriptor only when the grain
  answers to interfaces other than its own. See
  [`docs/deviations.md`](docs/deviations.md#functional--reducer-authoring).
- **Durable jobs** — `@thresh/durable-jobs`: sharded (time-bucketed), durable, at-least-once scheduled
  grain invocation, with per-silo concurrency control (limiter + slow-start + overload backoff),
  retry policy, `pollAfter` supervision, and membership-driven shard ownership with dead-silo
  adoption, poison protection and a claim ramp-up budget. Memory + Redis (Lua-CAS) shard stores;
  `useDurableJobHandler` + `runtime.scheduleJob`.
- **Activation rebalancer** — the adaptive, entropy-minimizing model, the distributed mechanism (load
  gathering + `migrateRandomActivations` + `runRebalanceCycle`), and the elected singleton worker +
  `useActivationRebalancing(options?)` builder surface + `RebalancingReport` + convergence e2e; the
  cluster self-levels skewed load toward balance.

## ✅ Shipped in the 2026-07-24 issue burn-down (#18–#37)

- **Observability breadth** — OTel meters across messaging, reminders, streams, durable-jobs,
  directory and storage ops (`thresh.*`, no-op safe), alongside the existing call filters,
  activation-path spans and `traceparent`/baggage propagation — now including the
  server→client observer-push direction.
- **Ambient cancellation & per-call deadlines** — `@thresh/core/abort`, `AbortSignal` + deadline
  threaded through invocation context, dispatchers, turn scheduler (admission-time preemption),
  `onDeactivate(reason, signal?)` and grain storage; composes with the explicit
  `GrainCancellationToken` mechanism.
- **Scheduler back-pressure & deactivation timeout** — bounded per-activation queues
  (soft-warn/hard-reject with `LimitExceededException`), stuck-turn watchdog, and an enforced
  `deactivationTimeout` that force-invalidates a hung activation.
- **Versioned serializer** — `$tsvv` schema version, surrogate registry with polymorphism
  resolution, `Set` support and a circular-reference guard; legacy payloads decode unchanged.
- **`@readOnly` dev-mode mutation guard** — `SiloConfig.readOnlyStateGuard` (persistent-state
  facets; breadth remainder in `todo.md`).
- **Directory hardening** — handoff ACK-delete, recovery retry/backoff, register gated on silo
  liveness (Orleans `RegisterCore` parity).
- **Transaction TM confirmation-worker keepalive** — periodic re-resolution of in-doubt
  prepared records with backoff.
- **Stream failure handling** — provider-wired failure handler, durable poison store, explicit
  producer registration, typed config errors, cluster-shared memory streams in `TestCluster`.
- **Fixes** — silo stop deactivates before closing transport (TestCluster teardown race),
  durable-jobs cross-silo forwarding + in-grain `RunId` dedup, callback-initiated timer
  change/dispose, `ReminderOptions` through the builder, client cumulative call deadline.

## 📐 Designed (not yet implemented)

- **Browser state replication** (beyond parity) — see [`docs/deviations.md`](docs/deviations.md).

## ⏸ Deferred

- **Runtime key-kind assertion** — `GrainFactory.getGrain` could assert the supplied key's kind
  against `GrainInterface.key` when the interface declares one. Held back deliberately: implicit
  stream subscriptions synthesise string keys for grains whose declared kind may be `integer`
  (`packages/streams/src/implicit-subscriptions.ts`), so enabling the check without first resolving
  that would break implicit subscription delivery. The type-level key kind is already enforced.
- **`GrainWith*Key` marker interfaces** — retained, and now aliases of `GrainKey<TKey>`, so
  existing declarations compile unchanged and resolve to the same key type
  (`KeyKindFromMarker` maps marker, intersection, and declared kind to one answer). Not deprecated:
  intersecting `GrainKey<TKey>` is the recommended form for a separately declared contract, and a
  fused definition declares `{ key: ... }` instead. Removing the residual no-op
  `extends GrainWithStringKey` from test fixtures is cosmetic and unscheduled; `packages/parity`
  keeps them permanently, along with its `I` prefixes and Orleans FQN interface names, because those
  are upstream identifiers used for 1:1 re-diff.
- Stream-backing polish (Phase 3 of
  [`docs/stream-backings-postgres-kafka.md`](docs/stream-backings-postgres-kafka.md)):
  LISTEN/NOTIFY wake-up for the Postgres queue, consumer-lag gauges, worked examples.
