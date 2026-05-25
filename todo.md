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
- [x] Thin `@kubernetes/client-node` glue feeding `WatchedEndpoints` — `createKubernetesClientSource`
      (lists + watches the headless Service's EndpointSlices by the `kubernetes.io/service-name`
      label) behind an `EndpointSliceSource` boundary; `KubernetesEndpointWatch` drives the
      aggregator and re-lists/re-watches on watch close. `pod-environment` reads the downward-API
      `SiloAddress`; `useKubernetesMembership` runs the watch's start/stop with the silo. The
      membership view always includes the local silo so a first/only pod can bootstrap (readiness
      gates on membership being healthy). Unit-tested sociably with a fake source.
- [x] Health endpoints (`/ready`, `/live`, `/startup`) — `HealthCheck` probe logic + `HealthServer`
- [x] Graceful drain — `GracefulShutdown` flips readiness then stops the node; `SIGTERM` handler
- [x] Hosting builder (`createSilo()…build()`) → `SiloHost` tying node + membership + transport +
      health + drain; `start()` flips readiness, `stop()` drains
- [x] Cluster e2e — `examples/k8s-silo` deploys a 3-silo `StatefulSet` (headless Service + RBAC +
      in-cluster Redis, see `examples/k8s-silo/deploy/`) and asserts the Phase-3 exit criteria
      against a real cluster: the cluster forms, calls route to one activation across pods, killing
      the host pod reactivates the grain on a survivor (state intact via Redis), and a rolling
      update preserves state. Opt-in and env-gated (`K8S_E2E=1`); verified on Docker Desktop
      Kubernetes. The silo runs from the image under vite-node (no build step).

## Phase 4 — Persistence

- [x] `GrainStorage` contract + `StateHolder`; `PersistentState` facet with etag optimistic
      concurrency; `InconsistentStateError`
- [x] In-memory provider (`MemoryGrainStorage`); `@persistentState` decorator records fields per
      instance for runtime injection
- [x] Runtime wiring — catalog `activateState` hook injects facets + reads them before `onActivate`;
      builder `addStorage`/`useMemoryStorage`; end-to-end test: state survives a silo restart
- [x] Redis provider — `RedisGrainStorage` (state as a Redis hash; conditional Lua scripts give the
      same etag optimistic-concurrency contract as the in-memory provider, atomic across silos);
      builder `addRedisStorage(name, { url, keyPrefix? })` connects on `start()` / disconnects on
      `stop()` via host `onStart`/`onStop` hooks. Integration-tested against a real Redis
      (skip-if-down): etag conflicts, value-codec round-trip, and state surviving a silo restart.
- [ ] Postgres provider — **deferred** (additional provider, not a parity gap; Redis is the shipped default)

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
- [x] Redis reminder table — `RedisReminderTable` (each reminder a Redis hash; a sorted set indexes
      them by uniform hash code so `readRange` hash-range ownership is a server-side query, including
      wrap-around; a per-grain set backs `readForGrain`; atomic Lua upsert/remove with etag CAS).
      Builder `useRedisReminders({ url, keyPrefix? })`. Integration-tested (skip-if-down): CAS,
      range/wrap queries, codec round-trip, firing through `LocalReminderService`, and a successor
      silo resuming a reminder from Redis.
- [ ] Postgres reminder table — **deferred** (additional provider, not a parity gap; Redis is the shipped default)

## Phase 6 — Event streams

- [x] Stream contracts (`StreamProvider`/`AsyncStream`/`StreamHandler`/`StreamSubscriptionHandle`/
      `SequenceToken`); in-memory provider with ordered delivery, per-subscription cursor + resume,
      rewind via start token, at-least-once redelivery, namespace/key isolation
- [x] Grain-facing wiring — `getStreamProvider` delivers `onNext` as a turn on the consumer's
      activation; durable subscriptions resume via `getSubscriptions`/`resume`; builder
      `useMemoryStreams`; end-to-end producer→consumer test
- [x] Redis Streams provider — `RedisStreamProvider` appends events with XADD (ordered, durable);
      each consumer's cursor is stored in Redis so a reactivated consumer resumes from where it left
      off, even on a different silo after a restart; delivery is a non-blocking XRANGE poll (shares
      the silo connection, works cross-silo) and the cursor advances only after `onNext` resolves
      (at-least-once). Builder `addRedisStreams(name, { url, keyPrefix? })`. Integration-tested
      (skip-if-down): ordered delivery, fan-out, namespace/key isolation, start-token rewind, and
      durable resume across a fresh provider replaying only the missed events.
- [x] Pulling agents / queue ownership over the ring ([ADR 0007](docs/adr/0007-stream-pulling-agents.md)).
      Streams are partitioned over a fixed set of physical Redis-Stream queues; each silo runs an
      agent per queue the ring assigns it; delivery routes to subscribers via a `StreamConsumer`
      system extension; the queue cursor commits after delivery (at-least-once) so a new owner resumes
      losslessly. Completes the Phase 6 exit criterion (queue ownership rebalances on membership
      change; a new owner resumes from the committed cursor). Slices:
  - [x] Slice 1 — queue model + durable per-queue cursor + `QueuePullingAgent` (pull a queue, deliver
        to a sink, commit the cursor). Integration-tested against real Redis, single process.
  - [x] Slice 2 — durable pub-sub subscription registry (`streamKey → subscriber grain ids`) in Redis.
  - [x] Slice 3 — `StreamConsumer` system extension + per-activation handler registry +
        `node.deliverStreamEvent` via the dispatcher; `RedisPullingStreamProvider` wired through
        `addRedisStreams` (superseding the per-consumer poll provider). Single-silo end-to-end:
        fan-out of a room's messages to multiple subscribed user grains, in order, with isolation.
        Each silo currently runs an agent for every queue (ownership is slice 4).
  - [x] Slice 4 — ring-based queue ownership + rebalance. Pure `ownedQueueIndices` maps each queue's
        ring point to an owning silo (disjoint + complete across the cluster); the provider's
        `refreshOwnership` is wired through a host `onOwnershipChange` hook called on start and every
        membership change (alongside reminder ownership). Multi-silo end-to-end (skip-if-down): the
        queue owner leaves the view and a surviving silo resumes delivery from the committed cursor —
        no gaps, no duplicate redelivery.

## Worked examples and docs

- [x] **Executable** worked example `examples/thermostat` — runnable entry point
      (`pnpm --filter @tsva/example-thermostat start`, via vite-node) exercising `@persistentState`,
      a reminder, and a telemetry stream over in-memory providers + in-process transport; a smoke
      test runs the full demo in the suite so it can't rot
- [x] Reconcile `docs/11` (public API), `docs/12`, `README` with the shipped API + getting-started/run docs
- [x] Docs accuracy pass — reconciled `docs/11` ("what is implemented today": `useMembership`,
      collection/refresh/`random` config, `@reducerState`, the `client` package) and `docs/12`
      (package list + `examples/*`) with the shipped surface

The goal is **Orleans parity** ([docs/13](docs/13-roadmap-and-phases.md)), tracked as a rolling
roadmap rather than a single v1 release. Phases 1–6 (core model, persistence, reminders, streams on
Redis, Kubernetes hosting) are shipped and verified. Remaining parity work is below, in priority
order, starting with transactions.

## Phase 7 — Cross-grain transactions ([ADR 0008](docs/adr/0008-cross-grain-transactions.md))

- [ ] Slice 1 — transaction context + boundaries: `TransactionOption` on method options; the
      proxy/dispatcher begins/joins a transaction and propagates the context through the request
      context across silos. In-memory TM, single silo, no durability. Failing test first: two grains
      updated in one transaction, an induced failure aborts both (no half-apply).
- [ ] Slice 2 — `TransactionalState<T>` facet: versioned state, per-transaction tentative writes,
      read/write version tracking; `performUpdate` / `performRead`. Sociable tests over the in-memory
      path.
- [ ] Slice 3 — optimistic serializable commit: TM prepare/commit/abort across participants;
      serializable conflict detection; cascading abort. End-to-end: a transfer across two account
      grains is atomic, concurrent transfers serialize, a conflicting transaction aborts.
- [ ] Slice 4 — durability + recovery: persist committed transactional state and commit records
      (Redis); resolve in-doubt transactions on restart; remote participants over the dispatcher.
      Multi-silo end-to-end: a silo dies mid-commit and the outcome stays consistent.

## Remaining for parity (after transactions)

- [ ] Grain-interface versioning — multiple interface versions live at once for heterogeneous rolling
      upgrades, with version-aware placement (Orleans' versioning).
- [ ] Implicit stream subscriptions — bind a grain type to a namespace and auto-subscribe by key, no
      explicit `subscribe` call (Orleans' `[ImplicitStreamSubscription]`).
- [ ] Directory range handoff — replace the phase-2 drop-and-rebuild with a versioned, lossless
      handoff on membership change (per [docs/06](docs/06-grain-directory-and-placement.md)).
- [ ] Observability (cross-cutting) — OpenTelemetry traces propagated via request context, metrics
      (activations, turn latency, directory hit rate, reminder/stream lag), and structured logs.

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
      publication of raised events to the grain's stream; event upcasting. **Deferred** — an addition
      beyond Orleans, not a parity item.

## Functional grains ([ADR 0009](docs/adr/0009-functional-grains.md))

- [x] Spike — `defineGrain(name, factory)` (functional counterpart of `@grain()`) plus
      `useReducerState` / `usePersistentState` hooks (counterparts of the field decorators). The
      factory runs once per activation with an explicit `ctx` (`id` / `runtime` / `getGrain`), keeps
      state in closures, and returns the interface methods + optional lifecycle. Produces a `Grain`
      subclass that registers/activates through the **unchanged** catalog, scheduler, proxy and
      facet-binding machinery, so class and functional styles coexist. Functional `examples/bank`
      account grain (shares `initialAccount` / `reduceAccount` verbatim) + end-to-end test: events
      fold to state, transfer moves funds, snapshot survives a restart, overdraft rejected.
- [ ] Decide whether functional becomes a documented/default style; if so, functional variants of
      `docs/02` + the examples, and `useReminder` / `useTimer` / `useActivate` sugar.

## Message-dispatch reducer grains ([ADR 0010](docs/adr/0010-message-dispatch-reducer-grains.md))

- [x] Spike — `defineReducerGrain(name, { initial, reduce })`: the whole wire surface is two fixed
      methods (`dispatch(action)` + read-only `query()`), so there is **no per-grain method table**
      (`defineGrainInterface`) to hand-write or generate — the `Action` union is the protocol and
      `<S, A>` carries the types. The reducer is pure and **effects are data** (Elm/Redux):
      `reduce(state, action) => { state, effects? }`; the runtime runs returned effects after folding
      and persisting the snapshot (`call(grain, key, action)` ships; reuses `GrainStorage`). Layered
      on `defineGrain` (ADR 0009), zero runtime change. `examples/bank` `account-reducer-grain` +
      end-to-end test (deposit/withdraw, transfer-as-effect, snapshot survives a restart, overdraft
      rejected by the pure reducer).
- [ ] More effect kinds (timer/reminder/stream-publish/self-dispatch) + an injectable effect
      interpreter for testing; decide on per-action invocation options (interleave/oneWay).

## Message dispatch as the substrate ([ADR 0011](docs/adr/0011-message-dispatch-substrate.md))

- [x] Make method-name dispatch the wire/runtime substrate: `InvocationRequest`/`Message` carry
      `method: string`, the activation invokes `instance[method](...args)`, and the runtime `Proxy`
      dispatches by the accessed property name. Removed the ordered method table — `methodId` and the
      `methods: [...]` array are gone; `defineGrainInterface(name, { options? })` is now a compile-time
      view (TS type + sparse options). `interfaceId` retained as internal routing/options/rehydration
      plumbing (no developer-facing method table). Proxy guards `then` so a ref is never thenable.
      Superseded the `methodId` portion of ADR 0001; reconciled docs/01/02/04/11. Full suite green.
- [ ] Optional follow-on: collapse `interfaceId` to a bare grain-type token (drop the registry,
      `resolveGrainType`, and the registration `interfaces: [...]` mapping).

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
- [x] `examples/k8s-silo` — a silo deployed to **real Kubernetes** (StatefulSet + headless Service +
      RBAC + in-cluster Redis), with a small HTTP-over-grain API so the e2e drives grains through
      the cluster. Asserts the Phase-3 exit criteria (cluster forms, single-activation routing,
      pod-kill reactivation, rolling-update state survival). Env-gated (`K8S_E2E=1`); verified on
      Docker Desktop Kubernetes.
- [x] `examples/cluster` — multi-silo end-to-end over the **real WebSocket transport** (previously
      only the in-process cluster test and a WebSocket unit test existed): cross-silo routing to one
      activation via directory CAS, and reactivation on a survivor when the hosting silo leaves the
      view. Added builder seams `useMembership(service)` (share one view across in-process silos) and
      `random` (deterministic placement) to `createSilo`.
