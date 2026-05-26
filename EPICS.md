# EPICS

Headline status of the **Orleans 10 parity** port (a TypeScript, reducer/functional-first virtual-actor
runtime). Epic-level only — see [`todo.md`](todo.md) for the per-slice breakdown,
[`docs/13`](docs/13-roadmap-and-phases.md) for scope/exit criteria, and [`docs/adr`](docs/adr) for the
decisions.

## ✅ Shipped

- [x] **Core actor model** — grains, identity, `Proxy` references, turn scheduler, activation
      lifecycle, call-chain reentrancy (Phase 1).
- [x] **Messaging & multi-silo** — WebSocket/in-process transport, message envelope, serializer,
      dispatcher, DHT grain directory + location cache, placement strategies, static membership (Phase 2).
- [x] **Kubernetes hosting** — EndpointSlice-watch membership, health probes, graceful drain, the
      `createSilo()` builder, and a real-cluster e2e (Phase 3).
- [x] **Persistence** — `PersistentState` facet, etag optimistic concurrency, memory + Redis providers
      (Phase 4).
- [x] **Timers & reminders** — in-memory timers; durable reminders with hash-range ownership and
      rebalancing; memory + Redis tables (Phase 5).
- [x] **Event streams** — `StreamProvider`/subscriptions, pulling agents, ring-based queue ownership,
      durable cursors, at-least-once delivery; memory + Redis Streams (Phase 6).
- [x] **Cross-grain ACID transactions** — declarative boundaries, `TransactionalState<T>` facet with
      timestamp-ordered wait-die locking, optimistic two-phase commit (TM elected from writers),
      durable storage (memory + Redis), cross-silo participants, and in-doubt recovery (Phase 7,
      [ADR 0008](docs/adr/0008-cross-grain-transactions.md)).
- [x] **Reducer grains & functional-first authoring** — `defineGrain` + hooks, `@reducerState`
      snapshot mode, `defineReducerGrain` dispatch grains (ADRs
      [0006](docs/adr/0006-reducer-grains.md)/[0009](docs/adr/0009-functional-grains.md)/[0010](docs/adr/0010-message-dispatch-reducer-grains.md)/[0011](docs/adr/0011-message-dispatch-substrate.md)).
- [x] **Grain call filters** — incoming, outgoing, and per-grain interception (auth, retries, the
      observability seam) ([ADR 0012](docs/adr/0012-grain-call-filters.md)).
- [x] **Observability** ([ADR 0013](docs/adr/0013-observability.md)) — ambient **request context**,
      **OpenTelemetry tracing**, **metrics** (call counter + duration histogram, activation gauge,
      directory-cache hit/miss), and **structured logging**, all on the call-filter seam and no-op
      without an SDK/logger. (Reminder/stream-lag gauges deferred as optional polish.)

## 🚧 In progress

- [~] **External client** — in-process gateway-routed client ships; higher-level gateway discovery +
      a WebSocket client e2e remain.
- [~] **Functional-first examples** — docs lead functional; a few examples are still `@grain()`
      classes to migrate (one kept as a living interop example).

## 📋 TODO — remaining for Orleans 10 parity

- [x] **Grain migration** — live-migrate an activation to another silo with state preserved
      (`IGrainMigrationParticipant`, `MigrateOnIdle`, directed placement).
- [ ] **Activation rebalancer** — proactively move activations across silos to balance load
      (`IActivationRebalancer`).
- [x] **Grain-interface versioning** — multiple interface versions coexist for heterogeneous rolling
      upgrades, with version-aware placement ([ADR 0014](docs/adr/0014-grain-interface-versioning.md)).
- [x] **Implicit stream subscriptions** — bind a grain type to a namespace and auto-subscribe by key.
- [x] **Directory range handoff** — versioned, lossless directory handoff on membership change
      (replacing the phase-2 drop-and-rebuild).
- [x] **Placement filters** — prune candidate silos by metadata before placement; additional
      placement strategies.
- [x] **Broadcast channels** — lightweight in-cluster pub/sub without the pulling-agent machinery
      ([ADR 0015](docs/adr/0015-broadcast-channels.md)).
- [ ] **Durable journaling (`DurableGrain`)** — Orleans 10 `Orleans.Journaling`; needs an ADR
      (overlaps reducer/persistent state).
- [ ] **Durable jobs** — Orleans 10 `Orleans.DurableJobs` (durable workflows); needs an ADR.

## 🔭 Beyond parity (future)

- [ ] **Browser state replication & browser-hosted grains** — replicate grain state to the browser and
      run permitted grains client-side, gated by a server-enforced trust model
      ([docs/13 "Beyond parity"](docs/13-roadmap-and-phases.md)); needs an ADR.

## ⏸ Deferred

- [ ] **Additional durable providers** — Postgres grain storage / reminder table and other stream
      backings, behind the same interfaces. Redis is the shipped default.
