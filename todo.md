# todo

Work items, grouped by the phase they belong to in
[`docs/13-roadmap-and-phases.md`](docs/13-roadmap-and-phases.md). Test-first, vertical slices.

## Scaffolding

- [x] pnpm workspace, `tsconfig.base.json`, Vitest + `unplugin-swc`, ESLint + Prettier
- [x] package skeletons: `@tsva/core`, `@tsva/runtime`, `@tsva/messaging`, `@tsva/directory`
- [x] fake clock test-support

## Phase 1 — single-silo core actor model

- [x] Slice 1: `GrainId`, `Guid`, key-kind markers, `defineGrainInterface`, invoke options
- [x] Slice 2: turn scheduler — serialized turns, read-only interleave, reentrancy-id admission,
      non-reentrant queues
- [x] Slice 3: activation lifecycle (sociable) — `Grain` base, decorators, catalog, `ActivationData`,
      collector, factory, local dispatcher, silo; activate-before-first-message, idle
      deactivation + reactivation
- [x] Slice 4: call-chain reentrancy — A→B→A shared id does not deadlock; default call queues

## Phase 2 — messaging, multi-silo, directory, placement

- [x] Slice 5: `Serializer` (JSON + MessagePack) incl. `GrainId`/`Guid`/`SiloAddress`/grain-ref-as-identity
- [x] Slice 6: `Message` envelope, in-process transport, correlation table — request/response + oneWay
- [x] Slice 7: consistent-hash ring — deterministic ownership, balance, fractional reshuffle on join/leave
- [x] Slice 8: partitioned directory CAS + location cache — race→one winner/loser forwards, cache
      hit, stale invalidation
- [x] Slice 9: placement strategies — random, preferLocal, activationCount, statelessWorker
- [x] Slice 10: static membership + distributed dispatcher + rebalancing — cross-silo routing,
      single activation via directory CAS, location cache, remove silo → ring rebalances + grains
      reactivate (in-process transport; WebSocket in slice 11)
- [x] Slice 11: WebSocket transport — cross-silo over real sockets, preamble handshake, clusterId mismatch rejected

## Phase 3 — Kubernetes membership and hosting

- [x] EndpointSlice parsing → ready silo set; watch-driven `KubernetesMembership` (versioned
      snapshots, ready = active, podUid distinguishes incarnations)
- [x] Transport-backed directory RPC — directory ops route to the owning silo as `system` messages
      over the transport (default; in-process peer kept for single-process tests)
- [x] `WatchedEndpoints` — aggregates incremental watch events (ADDED/MODIFIED/DELETED) into the
      ready set; drives `KubernetesMembership`
- [ ] Thin `@kubernetes/client-node` glue feeding `WatchedEndpoints` (needs the client dep + a cluster)
- [x] Health endpoints (`/ready`, `/live`, `/startup`) — `HealthCheck` probe logic + `HealthServer`
- [x] Graceful drain — `GracefulShutdown` flips readiness then stops the node; `SIGTERM` handler
- [x] Hosting builder (`createSilo()…build()`) → `SiloHost` tying node + membership + transport +
      health + drain; `start()` flips readiness, `stop()` drains
- [ ] kind cluster e2e — 3-silo `StatefulSet`, pod-kill reactivation, rolling update (needs a live cluster)

## Phase 4 — Persistence

- [x] `GrainStorage` contract + `StateHolder`; `PersistentState` facet with etag optimistic
      concurrency; `InconsistentStateError`
- [x] In-memory provider (`MemoryGrainStorage`); `@persistentState` decorator records fields per
      instance for runtime injection
- [x] Runtime wiring — catalog `activateState` hook injects facets + reads them before `onActivate`;
      builder `addStorage`/`useMemoryStorage`; end-to-end test: state survives a silo restart
- [ ] Redis (default) + Postgres providers (need real infra; integration-tested)

## Phase 5 — Timers and reminders

- [x] In-memory timers — `registerTimer` fires the callback as a turn via the injectable clock;
      fixed-rate periodic; cancelled on deactivation
- [x] Reminders core — `Remindable`/`TickStatus`, `ReminderTable` contract + in-memory table (etag),
      `LocalReminderService` with hash-range ownership, periodic firing, and silo-handoff
      (`refreshOwnership` re-reads the table)
- [x] Grain-facing wiring — `registerReminder`/`unregisterReminder` on the runtime; the silo's
      `onFire` reactivates the grain and delivers `receiveReminder` as a turn; builder `useReminders`;
      end-to-end test (grain registers → fires → delivered)
- [x] Multi-silo reminder ownership from the ring + rebalance on view change — each silo owns the
      hash ranges the consistent-hash ring assigns it (`ring.rangesFor`), refreshed on every
      membership change; a reminder registered on a non-owner is discovered by the owner via periodic
      table refresh; delivery routes through the dispatcher so a tick reaches the grain's single
      activation (not a second one on the owner). `Date` added to the wire codec for `TickStatus`.
- [ ] Redis (default) + Postgres reminder tables (need real infra; integration-tested)

## Phase 6 — Event streams

- [x] Stream contracts (`StreamProvider`/`AsyncStream`/`StreamHandler`/`StreamSubscriptionHandle`/
      `SequenceToken`); in-memory provider with ordered delivery, per-subscription cursor + resume,
      rewind via start token, at-least-once redelivery, namespace/key isolation
- [x] Grain-facing wiring — `getStreamProvider` delivers `onNext` as a turn on the consumer's
      activation; durable subscriptions resume via `getSubscriptions`/`resume`; builder
      `useMemoryStreams`; end-to-end producer→consumer test
- [ ] Redis Streams provider + pulling agents / queue ownership over the ring (need real infra)

## On approach to v1 completion

- [x] **Executable** worked example `examples/thermostat` — runnable entry point
      (`pnpm --filter @tsva/example-thermostat start`, via vite-node) exercising `@persistentState`,
      a reminder, and a telemetry stream over in-memory providers + in-process transport; a smoke
      test runs the full demo in the suite so it can't rot
- [x] Reconcile `docs/11` (public API), `docs/12`, `README` with the shipped API + getting-started/run docs
- [x] Docs accuracy pass — reconciled `docs/11` ("what is implemented today": `useMembership`,
      collection/refresh/`random` config, `@reducerState`, the `client` package) and `docs/12`
      (package list + `examples/*`) with the shipped surface
- [ ] Declare v1 done — gated on the infra-bound durable providers below (Redis/Postgres, k8s glue +
      kind e2e), which can't be verified without real infrastructure

## Reducer grains ([ADR 0006](docs/adr/0006-reducer-grains.md))

- [x] Snapshot mode — `@reducerState(name, { initial, reduce })` facet + `ReducerState<S, E>`
      contract; command handlers `raise` events folded through a pure reducer into immutable state;
      the folded state is persisted as a snapshot via the existing `GrainStorage` (events transient),
      bound before `onActivate` alongside `@persistentState`. Unit + end-to-end tests (fold, command
      validation, survives a silo restart).
- [x] Worked example `examples/bank` — accounts as reducer grains (deposit/withdraw/transfer);
      runnable demo + smoke test; events fold to immutable state, snapshot survives a silo restart
- [x] Reflect `@reducerState` in `docs/07` (persistence), `docs/11` (public API + examples), `README`
- [ ] Event-log mode — append-only `EventLog` provider + replay-from-snapshot on activation; opt-in
      publication of raised events to the grain's stream; event upcasting (needs real infra)

## External client

- [x] `@tsva/client` — gateway-routed `createClient({ clusterId, local, transport, gateway })`; not a
      silo (hosts no grains), forwards every `getGrain` call to a gateway silo and awaits the reply
      over a connection back to the client. Registers grains for interface→type resolution. In-process
      acceptance test (routing to a single activation, application-error propagation, unregistered
      interface rejected).
- [ ] Higher-level gateway discovery (`gateway: { url }`) + a WebSocket client e2e (needs the gateway
      Service shape from docs/10)

## Examples as acceptance tests

Runnable example apps that double as end-to-end acceptance tests, driving capabilities that
previously had only unit coverage. Outside-in / ATDD: failing example first, then make it pass.

- [x] `examples/greeter` — minimal getting-started grain; acceptance assertions for the Phase-1 exit
      criteria (activate-before-first-call, serialized turns, volatile state resets on idle
      reactivation). Doubles as the README quick-start.
- [x] `examples/chat` — stream fan-out to multiple distinct consumer grains: many users subscribe to
      one room stream; ordered delivery to all, room isolation, and durable resume per consumer.
- [x] Consumer-scoped stream subscriptions — a consumer reacquires its **own** durable subscription
      under fan-out (`getSubscriptions` filtered by the activation's grain id), driven by the chat
      durable-resume slice. Stream delivery is decoupled from `subscribe`/`resume` so a grain can
      subscribe mid-turn without deadlocking against its own queue.
- [x] Idle collection in the hosted (`ClusterNode`) path — `createSilo` now starts an
      `ActivationCollector` and accepts `collectionAgeSeconds` / `collectionIntervalSeconds`
      (previously only the bare `Silo` swept idle activations), surfaced by the greeter/chat
      reactivation slices.
- [x] `examples/cluster` — multi-silo end-to-end over the **real WebSocket transport** (previously
      only the in-process cluster test and a WebSocket unit test existed): cross-silo routing to one
      activation via directory CAS, and reactivation on a survivor when the hosting silo leaves the
      view. Added builder seams `useMembership(service)` (share one view across in-process silos) and
      `random` (deterministic placement) to `createSilo`.
