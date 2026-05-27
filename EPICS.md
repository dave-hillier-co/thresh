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
  `createSilo()` builder, and a real-cluster e2e.
- **Persistence** — `PersistentState` facet with etag concurrency; memory + Redis + Postgres providers.
- **Timers & reminders** — in-memory timers; durable reminders with hash-range ownership and
  rebalancing; memory + Redis + Postgres tables.
- **Event streams** — `StreamProvider`/subscriptions, pulling agents, ring-based queue ownership,
  durable cursors, at-least-once delivery, implicit subscriptions; memory + Redis Streams.
- **Cross-grain ACID transactions** — `TransactionalState<T>` with timestamp-ordered wait-die
  locking, optimistic two-phase commit (TM elected from writers), durable storage, cross-silo
  participants, in-doubt recovery.
- **Durable journaling (`DurableGrain`)** — a per-grain `StateMachineManager` over
  `DurableValue`/`DurableDictionary`/`DurableList` that journals each mutation to an append-only log
  and replays it on activation, with snapshot/compaction; a `JournalStorage` seam (memory + Redis),
  `useDurable*` hooks + `@durable*` decorators, and `useMemoryJournaling()`/`addRedisJournaling()`.
  Separate from the reducer snapshot facet and `PersistentState`.
- **Grain migration** — live activation move with state preserved (`IGrainMigrationParticipant`,
  `MigrateOnIdle`, directed placement).
- **Grain-interface versioning** — versions coexist for heterogeneous rolling upgrades, with
  version-aware placement.
- **Broadcast channels** — lightweight in-cluster pub/sub to implicit subscribers.
- **Grain call filters** — incoming, outgoing, and per-grain interception.
- **Observability** — request context, OpenTelemetry tracing/metrics, structured logging, on the
  call-filter seam; no-op without an SDK.
- **External client** — gateway-routed client with gateway discovery + failover, verified in-process
  and over real WebSocket sockets.
- **Reducer & functional-first authoring** — `defineGrain` + hooks, snapshot reducers, dispatch
  grains. Every example grain is functional (one `@grain()` class kept on purpose as the living
  interop example).
- **Durable jobs** — `@tsva/durable-jobs`: sharded (time-bucketed), durable, at-least-once scheduled
  grain invocation, with per-silo concurrency control (limiter + slow-start + overload backoff),
  retry policy, `pollAfter` supervision, and membership-driven shard ownership with dead-silo
  adoption, poison protection and a claim ramp-up budget. Memory + Redis (Lua-CAS) shard stores;
  `useDurableJobHandler` + `runtime.scheduleJob`.
- **Activation rebalancer** — the adaptive, entropy-minimizing model, the distributed mechanism (load
  gathering + `migrateRandomActivations` + `runRebalanceCycle`), and the elected singleton worker +
  `useActivationRebalancing(options?)` builder surface + `RebalancingReport` + convergence e2e; the
  cluster self-levels skewed load toward balance.

## 📐 Designed (not yet implemented)

- **Browser state replication** (beyond parity) — see [`docs/deviations.md`](docs/deviations.md).

## ⏸ Deferred

- Additional stream backings behind the existing interfaces (Redis is the default).
