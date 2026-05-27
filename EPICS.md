# EPICS

Status board for the **Orleans 10 parity** port (a TypeScript, reducer/functional-first virtual-actor
runtime). See [`todo.md`](todo.md) for outstanding work, [`docs/`](docs/) for the design, and
[`docs/adr`](docs/adr) for the decisions.

## ✅ Shipped

- **Core actor model** — grains, identity, `Proxy` references, turn scheduler, activation lifecycle,
  call-chain reentrancy.
- **Messaging & multi-silo** — WebSocket/in-process transport, message envelope, serializer,
  dispatcher, DHT directory + location cache with versioned lossless range handoff, static membership.
- **Placement** — random / prefer-local / activation-count / stateless-worker / silo-role /
  resource-optimized strategies, metadata placement filters, and version-aware placement.
- **Kubernetes hosting** — EndpointSlice-watch membership, health probes, graceful drain, the
  `createSilo()` builder, and a real-cluster e2e.
- **Persistence** — `PersistentState` facet with etag concurrency; memory + Redis + Postgres providers.
- **Timers & reminders** — in-memory timers; durable reminders with hash-range ownership and
  rebalancing; memory + Redis + Postgres tables.
- **Event streams** — `StreamProvider`/subscriptions, pulling agents, ring-based queue ownership,
  durable cursors, at-least-once delivery, implicit subscriptions; memory + Redis Streams.
- **Cross-grain ACID transactions** ([ADR 0008](docs/adr/0008-cross-grain-transactions.md)) —
  `TransactionalState<T>` with timestamp-ordered wait-die locking, optimistic two-phase commit
  (TM elected from writers), durable storage, cross-silo participants, in-doubt recovery.
- **Durable journaling (`DurableGrain`)** ([ADR 0019](docs/adr/0019-durable-journaling.md)) — a
  per-grain `StateMachineManager` over `DurableValue`/`DurableDictionary`/`DurableList` that journals
  each mutation to an append-only log and replays it on activation, with snapshot/compaction; a
  `JournalStorage` seam (memory + Redis), `useDurable*` hooks + `@durable*` decorators, and
  `useMemoryJournaling()`/`addRedisJournaling()`. Separate from the reducer snapshot facet and
  `PersistentState`.
- **Grain migration** — live activation move with state preserved (`IGrainMigrationParticipant`,
  `MigrateOnIdle`, directed placement).
- **Grain-interface versioning** ([ADR 0014](docs/adr/0014-grain-interface-versioning.md)) — versions
  coexist for heterogeneous rolling upgrades, with version-aware placement.
- **Broadcast channels** ([ADR 0015](docs/adr/0015-broadcast-channels.md)) — lightweight in-cluster
  pub/sub to implicit subscribers.
- **Grain call filters** ([ADR 0012](docs/adr/0012-grain-call-filters.md)) — incoming, outgoing, and
  per-grain interception.
- **Observability** ([ADR 0013](docs/adr/0013-observability.md)) — request context, OpenTelemetry
  tracing/metrics, structured logging, on the call-filter seam; no-op without an SDK.
- **External client** — gateway-routed client with gateway discovery + failover, verified in-process
  and over real WebSocket sockets.
- **Reducer & functional-first authoring** (ADRs [0006](docs/adr/0006-reducer-grains.md)/[0009](docs/adr/0009-functional-grains.md)/[0010](docs/adr/0010-message-dispatch-reducer-grains.md)/[0011](docs/adr/0011-message-dispatch-substrate.md))
  — `defineGrain` + hooks, snapshot reducers, dispatch grains. Every example grain is functional
  (one `@grain()` class kept on purpose as the living interop example).
- **Activation rebalancer** ([ADR 0016](docs/adr/0016-activation-rebalancer.md)) — the adaptive,
  entropy-minimizing model (slice 1), the distributed mechanism (load gathering +
  `migrateRandomActivations` + `runRebalanceCycle`, slice 2a), and the elected singleton worker +
  `useActivationRebalancing(options?)` builder surface + `RebalancingReport` + convergence e2e
  (slice 2b) all ship; the cluster self-levels skewed load toward balance.

## 📐 Designed (not yet implemented)

- **Durable jobs** ([ADR 0018](docs/adr/0018-durable-jobs.md)) — sharded, durable, at-least-once
  scheduled execution.
- **Browser state replication** ([ADR 0017](docs/adr/0017-browser-state-replication.md), beyond parity).

## ⏸ Deferred

- Additional stream backings behind the existing interfaces (Redis is the default).
