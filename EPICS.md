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

## 🚧 Partial — parity gaps in flight

- **Observability** — request context, OpenTelemetry tracing surface, W3C `traceparent`
  propagation and structured logging are wired on the call-filter seam (no-op without an SDK), but
  the runtime itself (catalog, directory, persistence, messaging, reminders, streams) is not yet
  instrumented with the meter set, and spans do not yet carry `exception.*` attributes.
- **Cancellation & per-call deadlines** — Orleans threads `CancellationToken` through every call,
  deactivation, and storage operation; the TS port does not yet.
- **Scheduler back-pressure & deactivation timeout** — per-activation queues are unbounded; no
  stuck-turn detection (`MaxRequestProcessingTime`) and no enforced `onDeactivate` timeout.
- **Serializer versioning & polymorphism** — the value codec has no schema version, surrogate
  types, polymorphism resolution, `Map`/`Set` support or circular-reference guard.
- **Grain observers & extensions** — no typed observer/client-callback surface and no
  `IGrainExtension` mechanism.
- **`StatelessWorker` enforcement** — the option is parsed but not honored (single-activation
  semantics still apply).
- **`@readOnly` runtime check** — advisory only; no mutation check even in dev.
- **Directory handoff ACK & cleanup** — handoff snapshots are best-effort pull with no ACK-delete
  loop; retained indefinitely if a successor crashes pre-pull.
- **Transaction TM confirmation-worker keepalive** — in-doubt prepared records on remote
  participants are only resolved once at activation; no periodic TM ping.

## 📐 Designed (not yet implemented)

- **Browser state replication** (beyond parity) — see [`docs/deviations.md`](docs/deviations.md).

## ⏸ Deferred

- Additional stream backings behind the existing interfaces (Redis is the default).
