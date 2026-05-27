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
- [x] Postgres provider — `PostgresGrainStorage` (one row per state with an `etag` column; a
      conditional `INSERT ... ON CONFLICT DO UPDATE ... WHERE etag = $expected` gives the same etag
      optimistic-concurrency contract as the in-memory/Redis providers, atomic across silos); builder
      `addPostgresStorage(name, { connectionString, tableName? })` creates the table on `start()` /
      closes the pool on `stop()` via host `onStart`/`onStop` hooks. Integration-tested against a real
      Postgres (skip-if-down): etag conflicts, value-codec round-trip, and state surviving a silo
      restart.

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
- [x] Postgres reminder table — `PostgresReminderTable` (each reminder a row keyed by
      `(grain_id, name)` with an indexed `hash` column so `readRange` hash-range ownership is a
      server-side query, including wrap-around; a `grain_id` lookup backs `readForGrain`;
      unconditional upsert with a fresh etag and an etag-CAS remove). Builder
      `usePostgresReminders({ connectionString, tableName? })`. Integration-tested (skip-if-down):
      CAS, range/wrap queries, codec round-trip, firing through `LocalReminderService`, and a
      successor silo resuming a reminder from Postgres.

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

- [x] ADR 0008 revised to the faithful Orleans protocol (timestamp-ordered optimistic + wait-die,
      TM elected from the write participants, `PrepareAndCommit`/`Prepared`/`Cancel` message set,
      `CausalClock`); added `Orleans.Transactions/*` source citations. Status: Accepted.
- [x] Slice 1 — transaction context + boundaries: `transaction: TransactionOption` on
      `InvokeMethodOptions`; `TransactionInfo` + `AccessCounter` + `TransactionParticipant` in core;
      `req.transaction` propagated via `InvocationContext` (seeded in `activation.invoke`); the proxy
      (`grain-factory.buildProxy`) begins/joins per option and commits/aborts at the root boundary;
      per-silo `TransactionAgent` + `CausalClock` in `@tsva/runtime` wired into `Silo`/`ClusterNode`;
      minimal `TransactionalValue` seed + `currentTransaction`/`requireTransaction` in new
      `@tsva/transactions`. Test (`transaction-boundary.test.ts`): two-grain transfer commits
      atomically, an overdraft aborts both (no half-apply), a `join` method outside a transaction is
      rejected. Cross-silo `RequestContext` propagation + participant merging deferred to Slice 3.
- [x] Slice 2 — `TransactionalState<T>` facet: `performRead`/`performUpdate`, committed version with a
      dense sequence id + a single per-transaction tentative copy, `AccessCounter` read/write tracking,
      enlists as a participant on first access (`TransactionalStateImpl`). Wait-die reader-writer lock
      (`reader-writer-lock.ts`, ported from Orleans `State/ReaderWriterLock.cs`): timestamp-ordered,
      reads share, writes exclusive, older waits / younger dies (`TransactionAbortedError`).
      `@transactionalState` decorator + `useTransactionalState` hook + `bindTransactionalStates`
      activator wired into the `silo-builder` `stateBinder` (runs even without a storage provider).
      Tests: wait-die lock unit (6), facet sociable (tentative invisibility/commit/abort/own-write/
      younger-dies), and hosting end-to-end through `createSilo` (class + functional, commit + abort).
- [x] Slice 3 — optimistic serializable commit (single-silo): `TransactionParticipant` grew a
      `prepare` phase; `TransactionAgent.resolve` runs faithful two-phase commit — collate
      participants, elect the TM from the writers, prepare all, then commit all, or cascade-abort and
      raise `TransactionAbortedError`. `TransactionalStateImpl.prepare` validates/stages.
      `TransactionAbortedError` moved to `@tsva/core/errors`. End-to-end: atomic transfer (hosting +
      `examples/bank` transactional account/teller), wait-die concurrency (older commits, younger
      aborts — `transaction-concurrency.test.ts`), overdraft rolls back both. **Note:** routing
      TM/resource calls over the dispatcher for *remote* participants folds into Slice 4 (where
      durability + multi-silo recovery live); single-silo participants are in-process objects.
- [~] Slice 4 — durability + recovery (in progress):
  - [x] 4a — durable transactional storage: `TransactionalStateStorage` contract
        (`load`/`store` with pending states, `commitUpTo`/`abortAfter`, commit-records metadata;
        ported from Orleans `ITransactionalStateStorage`) + `MemoryTransactionalStorage`.
        `TransactionalStateImpl` is now storage-backed: `load` on activation, `prepare` stages a
        pending record, `commit` promotes it (`commitUpTo`), `abort` drops it. Builder
        `addTransactionalStorage` / `useMemoryTransactionalStorage` (+ a per-silo in-memory default);
        `bindTransactionalStates` is async and loads. Test: committed state survives a silo restart
        via a shared store; an aborted write persists nothing (`transaction-durability.test.ts`).
  - [x] 4b — Redis transactional storage provider: `RedisTransactionalStorage` keeps the record
        (committed + pending list + commit-records metadata) as a Redis hash (`data` JSON + `etag`);
        `store` reads, applies the shared `applyStore` deltas, and writes back under a conditional
        Lua CAS — same etag contract as the memory provider (extracted to
        `transactional-storage-apply.ts` so both share semantics). Builder
        `addRedisTransactionalStorage`. Integration test (skip-if-down): prepare/commit/load, stale
        etag rejected, committed state survives a silo restart via Redis.
  - [x] 4c — remote participants over the dispatcher: the participant set is now a serializable
        `ParticipantId` (grainId + state name) + `AccessCounter`, with an optional live reference for
        local participants. The transaction context (id/timestamp/readOnly) rides the request
        `RequestContext`; the callee's enlisted participants ride back on the reply
        (`transactionParticipants`) and merge into the caller's transaction — even on an error reply,
        so an abort releases remote locks. The agent drives a local participant directly and a
        merged-in remote one over the dispatcher via the `TransactionResource` system extension
        (`activation.invokeTransactionResource` routes it to the named state). Retired the vestigial
        `TransactionalValue` seed (superseded by the facet). Multi-silo e2e
        (`transactions-cluster.test.ts`): a transfer spanning accounts on two silos commits
        atomically; an overdraft aborts both and frees the remote lock.
  - [x] 4d — in-doubt recovery: `prepare` records the elected TM (`ParticipantId`) in each durable
        pending record; the TM durably writes a commit record *before* participants commit (the atomic
        commit point); on activation a resource with an in-doubt pending record asks the TM
        (`status`, over the dispatcher, or directly if it is its own TM) and commits or aborts
        accordingly. The stored record round-trips through the value-codec so a pending record's TM
        `GrainId` and state survive (both providers). Test (`transaction-recovery.test.ts`, seeds a
        post-crash in-doubt state): a record whose TM recorded the commit recovers to committed; one
        with no commit record recovers to aborted.

  **Phase 7 (cross-grain ACID transactions) complete:** boundaries, the `TransactionalState<T>` facet
  with wait-die locking, optimistic two-phase commit, durable storage (memory + Redis), cross-silo
  participants, and in-doubt recovery — all shipped and green.

## Remaining for parity (after transactions)

- [x] Grain call filters ([ADR 0012](docs/adr/0012-grain-call-filters.md)) — incoming/outgoing
      interception (auth, retries, trace propagation); the observability seam. **Shipped** (incoming,
      outgoing, per-grain).
  - [x] Slice 1 — incoming filters: `GrainCallContext` (target/source/interface/method, mutable
        `args`/`result`, `invoke()`) + `runCallFilters` pipeline in core; the activation runs the
        pipeline around grain-method dispatch (system extensions bypass it); builder
        `addIncomingCallFilter` threaded through `ClusterNode`→catalog→activation. Unit (pipeline:
        order, arg rewrite, short-circuit, result wrap, error) + hosting e2e (wrap/observe/order, arg
        rewrite, auth short-circuit).
  - [x] Slice 2 — outgoing filters at the proxy: `GrainFactory.setOutgoingCallFilters`; `buildProxy`
        wraps the dispatch (incl. the transaction boundary) in `runCallFilters`; builder
        `addOutgoingCallFilter` threaded through `ClusterNode`. Hosting e2e (wrap/observe target+method,
        client-side-cache short-circuit).
  - [x] Slice 3 — per-grain incoming filters: a grain exposes `[INCOMING_CALL_FILTER]` (symbol) and it
        runs innermost (after silo-wide filters, before the method); `defineGrain` installs
        symbol-keyed behaviour members so functional grains can declare it too. Hosting e2e: class +
        functional self-filter, ordering relative to a silo filter.
- [~] Grain migration / activation rebalancer (Orleans 10 core runtime).
  - [x] Slice 1 — live migration on idle: `IGrainMigrationParticipant` (`onDehydrate`/`onRehydrate`)
        in core; `runtime.migrateOnIdle(targetSilo?)`; the idle-collection sweep moves a migrating
        activation to another silo (directed via `chooseMigrationTarget`, else placement strategy over
        other silos) instead of deactivating it. The source dehydrates on a turn and rejects further
        calls as stale; the target rehydrates and claims the directory entry via CAS
        (`register(newAddr, sourceAddr)`); `@persistentState` facets auto-participate (value+etag move
        in the bag, target skips the storage read). Unit (participant guard/bag, directed target
        choice, dehydrate/reject-after) + multi-silo e2e (directed + strategy-chosen, single-silo
        fallback) + hosting e2e (unflushed persistent state preserved across the move).
  - [~] Activation rebalancer (`IActivationRebalancer`) — proactively rebalance activations across
        silos (Orleans 10 `Orleans.Runtime/Placement/Rebalancing/*`). [ADR 0016](docs/adr/0016-activation-rebalancer.md).
    - [x] Slice 1 — the decision model: a pure, faithful port of Orleans' adaptive entropy-minimizing
          algorithm (`rebalancer-model.ts`): `shannonEntropy` / `clusterImbalance` of the per-silo load
          distribution, `adaptiveScaling` (cycle/silo weights), `formSiloPairs` (low↔high), and
          `planCycle(snapshot, options, state)` → `{ moves, imbalance, nextState, stop? }` (skip <2
          silos, complete when balanced, stagnate when entropy barely changes, else scaled per-pair
          migrations). Load is a single scalar = activation count (uniform-memory case; we don't gossip
          per-silo memory yet — documented divergence). Pure/deterministic, 16 unit tests incl. a
          full-session convergence simulation.
    - [x] Slice 2a — the mechanism: cross-silo activation-count reporting (a `system: "load"` RPC +
          `gatherClusterLoad` keyed by ring key); `migrateRandomActivations(target, count)` (directed
          immediate migration of N random live activations, reusing the live-migration path then
          deactivating locally), reachable on a peer via a `system: "rebalance"` RPC; and
          `runRebalanceCycle(state)` that gathers load, runs `planCycle`, and executes each move by
          telling the busier silo to shed to its paired quieter one. Multi-silo test: load gathering,
          directed migration counts, and a cycle shedding from a [10,2]-skewed pair.
    - [ ] Slice 2b — automation: an elected singleton worker driving `runRebalanceCycle` on a timer
          (sessions/cycles, due-time/backoff), builder `useActivationRebalancing(options?)`, a
          `RebalancingReport` + suspend/resume, and a convergence e2e over a running cluster.
- [x] Grain-interface versioning — multiple interface versions live at once for heterogeneous rolling
      upgrades, with version-aware placement (Orleans' versioning). [ADR 0014](docs/adr/0014-grain-interface-versioning.md).
  - [x] `GrainInterface.version` (default 1; id stays name-derived) via `defineGrainInterface(name, { version })`;
        `interfaceVersion` threaded `InvocationRequest`→`Message`→back (absent ⇒ 1).
  - [x] Policy abstractions in core: `CompatibilityDirector` (`backwardCompatible`/`strict`),
        `VersionSelectorStrategy` (`latest`/`all`/`minimum`), `SiloManifest`/`InterfaceVersionEntry`.
  - [x] Per-silo manifest from registered interfaces; lazy cross-silo `system: "manifest"` RPC
        (mirrors directory RPC), cached, cleared on membership change.
  - [x] Version-aware placement pre-filter in `DistributedDispatcher.placeAndInvoke` (best-effort
        fallback to all candidates when none compatible); inert unless versioning is active.
  - [x] Host surface `createSilo().useVersioning({ compatibility, selector })`.
  - [x] Tests: director/selector units, `filterByVersion` unit, multi-silo `versioning.cluster.test.ts`
        (v2→v2 silo, v1→v2 silo, strict + best-effort fallback, selectors, inert v1-only cluster).
- [x] Implicit stream subscriptions — bind a grain type to a namespace and auto-subscribe by key, no
      explicit `subscribe` call (Orleans' `[ImplicitStreamSubscription]`). A grain type declares
      namespaces via `@implicitStreamSubscription(ns)` (class) or `defineGrain`'s
      `implicitSubscriptions` (functional); a grain with key `K` is auto-subscribed to `(ns, K)` and
      exposes its handler under the `STREAM_SUBSCRIPTION_OBSERVER` symbol (lazily resolved on first
      delivery, mirroring Orleans' `IStreamSubscriptionObserver`). The pulling agent's fan-out adds
      implicit subscribers (the `ClusterNode`'s namespace→grain-type map, synthesizing `(type, K)`
      grain ids) alongside the registry's explicit subscribers, deduplicated; in-memory provider
      stays explicit-only. Tests: metadata/observer unit, pure `implicitSubscriberIds`, dispatcher
      delivery (class + functional, no-observer drop), and a Redis pulling-agent e2e (skip-if-down).
- [x] Placement filters + metadata-aware strategies — `PlacementFilterStrategy` layer
      (`PlacementFilter` + `MetadataMatchFilter`, Orleans `PreferredMatchSiloMetadataPlacementFilter`)
      prunes candidate silos by metadata before the strategy runs, applied in the dispatcher before
      `choose`; `SiloRoleBasedPlacement` and `ResourceOptimizedPlacement` strategies. Silo metadata
      rides `SiloMember.metadata` (config + `StaticMembershipService` resolver); `PlacementContext`
      gained `siloMetadata`/`resourceStats` accessors. Unit + dispatcher + multi-silo integration
      tests. Follow-ups: populate `SiloMember.metadata` from Kubernetes pod labels; report remote
      `resourceStats` via membership gossip (today a peer's load is zero/metadata-only).
- [x] Directory range handoff — replaced the phase-2 drop-and-rebuild with a versioned, lossless
      handoff on membership change (per [docs/06](docs/06-grain-directory-and-placement.md)): on a
      view change the silo losing a range sets its live entries aside (`LocalDirectoryPartition.drain`)
      and the new owner recovers them by pulling from the previous owner (a `recover` directory op,
      merged register-if-absent); directory ops carry the membership view version so a behind owner
      self-advances and a stale caller is redirected (`staleView` rejection → refresh + re-resolve),
      and owned reads wait on in-flight recovery so a join no longer reactivates the moved grains.
- [x] Broadcast channels — lightweight in-cluster pub/sub without the pulling-agent / cursor machinery
      (Orleans 10 `Orleans.BroadcastChannel/*`). [ADR 0015](docs/adr/0015-broadcast-channels.md).
      `ChannelId` (namespace+key) + `BroadcastChannelProvider`/`Writer` contracts in core; a grain type
      declares implicit subscriptions via `@implicitChannelSubscription(ns)` (class) or `defineGrain`'s
      `implicitChannelSubscriptions` (functional), tracked in a `broadcastSubscriptions` metadata field
      kept separate from streams'. `publish` resolves the namespace's implicit subscribers (the
      `ClusterNode` namespace→grain-type map, synthesizing `(type, key)` ids) and fans out over the
      dispatcher as a `BroadcastConsumer` system extension; the activation runs the grain's
      `BROADCAST_CHANNEL_OBSERVER` handler as a turn (lazily resolved, mirroring streams). Host surface
      `createSilo().useBroadcastChannels(name)`; grains reach it via `runtime.getBroadcastChannelProvider`.
      Delivery awaits subscribers (one deliberate divergence from Orleans' fire-and-forget default).
      Tests: `channelKey` unit, namespace→types registration, dispatcher fan-out (class + functional +
      producer-grain publish, no-observer drop).
- [ ] Durable journaling (`DurableGrain`) — Orleans 10 `Orleans.Journaling`; needs an ADR (overlaps
      the reducer/persistent-state model). Next ADR slot (0017).
- [~] Durable jobs — Orleans 10 `Orleans.DurableJobs`: a **sharded, durable, at-least-once
      scheduled-execution** engine (one-shot grain invocations bucketed by due time, with retries,
      per-silo concurrency control, slow-start, and crash-failover with poison-shard protection) —
      **not** a workflow/replay engine despite the name. Designed in
      [ADR 0018](docs/adr/0018-durable-jobs.md) as `@tsva/durable-jobs`, layered on the runtime the
      way `@tsva/reminders` is (per-silo manager + durable shard store + memory/Redis backings; a
      `DURABLE_JOB_HANDLER` symbol handler; `runtime.scheduleJob`/`cancelJob`; `useDurableJobs`).
      Design only — implementation pending.
  - [ ] Slice 1 — pure model: shard-key bucketing, the due-time job queue (cancel/retry), the default
        retry policy, and the claim-budget computation, all pure + fake-clock unit-tested.
  - [ ] Slice 2 — single-silo e2e on the memory store: a scheduled job fires the target's
        `DURABLE_JOB_HANDLER` as a turn at due time; complete/throw-retry/cancel/`pollAfter`;
        concurrency limit + overload backoff.
  - [ ] Slice 3 — durable store + restart: Redis `JobShardStore`; a job survives silo restart and
        re-fires (at-least-once); memory store asserted dev/test-only.
  - [ ] Slice 4 — multi-silo ownership & failover (kind e2e): shard claim/adoption via membership,
        poison-shard protection, claim ramp-up, graceful release on drain.
- [~] Observability (cross-cutting) — OpenTelemetry traces propagated via request context, metrics
      (activations, turn latency, directory hit rate, reminder/stream lag), and structured logs.
  - [x] Slice 1 — ambient request context (Orleans `RequestContext`): `requestContext.get/set/getAll`
        over a string→string header bag that flows along the call chain in-process
        (`InvocationContext.headers`) and across silos (`Message.requestContext.headers`); the proxy
        copies ambient headers onto outgoing requests, the activation seeds them per turn. The carrier
        for W3C trace context + app baggage. Test: header propagates grain→grain, in-process + cross-silo.
  - [x] Slice 2 — tracing call filters: new `@tsva/observability` package (dep `@opentelemetry/api` +
        `@opentelemetry/core`); `tracingFilters()` opens CLIENT (outgoing) / SERVER (incoming) spans
        with rpc attributes, records exceptions/status; W3C trace context injected/extracted via the
        `GrainCallContext.headers` carrier (threaded proxy↔req↔message). Builder `useTracing()`
        ([ADR 0013](docs/adr/0013-observability.md)). Test with `sdk-trace-base` +
        `context-async-hooks` + `InMemorySpanExporter`: grain→grain call yields CLIENT+SERVER spans on
        one trace (SERVER child of CLIENT), and error status/exception recorded on throw.
  - [x] Slice 3 — metrics: `metricsFilters()` (incoming call filter) records a `tsva.grain.calls`
        counter (interface/method/status) + `tsva.grain.call.duration` histogram via
        `@opentelemetry/api` metrics; builder `useMetrics()`. Test with `@opentelemetry/sdk-metrics`
        `InMemoryMetricExporter`: a call records the counter + histogram with rpc attributes.
  - [~] Slice 4 — runtime gauges: `registerRuntimeMetrics` adds the `tsva.activations` observable
        gauge over `node.activationCount()` (catalog count), registered by `useMetrics()` at build and
        unregistered on stop. Test (sdk-metrics): the gauge reports the live activation count.
        Remaining: directory hit-rate + reminder/stream-lag gauges (need cache/service instrumentation).
  - [x] Slice 5 — structured logging: `Logger` contract in core (`debug`/`info`/`warn`/`error(msg,
        fields)`, `noopLogger`); `loggingFilter(logger)` logs each call with structured fields
        (grain/interface/method/durationMs/status) at info, failures at error; `consoleLogger()` (JSON
        lines); builder `useLogging(logger)`. Test: a capturing logger records success + error.
  - [x] Slice 6 — directory hit-rate gauge: `LocationCache` counts hits/misses (`.stats`); node
        exposes `directoryCacheStats()`; `registerRuntimeMetrics` adds `tsva.directory.cache.hits`/
        `.misses` observable counters. Test (sdk-metrics): counters report after directory lookups.
  - [ ] (deferred polish) reminder/stream-lag gauges — fuzzy timing semantics, low value.

## Reducer grains ([ADR 0006](docs/adr/0006-reducer-grains.md))

- [x] Snapshot mode — `@reducerState(name, { initial, reduce })` facet + `ReducerState<S, E>`
      contract; command handlers `raise` events folded through a pure reducer into immutable state;
      the folded state is persisted as a snapshot via the existing `GrainStorage` (events transient),
      bound before `onActivate` alongside `@persistentState`. Unit + end-to-end tests (fold, command
      validation, survives a silo restart).
- [x] Worked example `examples/bank` — accounts as reducer grains (deposit/withdraw/transfer);
      runnable demo + smoke test; events fold to immutable state, snapshot survives a silo restart
- [x] Reflect `@reducerState` in `docs/07` (persistence), `docs/11` (public API + examples), `README`

## Functional grains ([ADR 0009](docs/adr/0009-functional-grains.md))

- [x] Spike — `defineGrain(name, factory)` (functional counterpart of `@grain()`) plus
      `useReducerState` / `usePersistentState` hooks (counterparts of the field decorators). The
      factory runs once per activation with an explicit `ctx` (`id` / `runtime` / `getGrain`), keeps
      state in closures, and returns the interface methods + optional lifecycle. Produces a `Grain`
      subclass that registers/activates through the **unchanged** catalog, scheduler, proxy and
      facet-binding machinery, so class and functional styles coexist. Functional `examples/bank`
      account grain (shares `initialAccount` / `reduceAccount` verbatim) + end-to-end test: events
      fold to state, transfer moves funds, snapshot survives a restart, overdraft rejected.
- [x] Decided: **functional is the documented default**; the class + decorator form is retained as
      the substrate / interop surface. ADR 0009/0010 bumped to Accepted; README + `docs/01`/`02`/`07`/
      `08`/`09`/`11`/`12`/`13` reoriented functional-first with a short class-substrate note; the
      `examples/bank` class account grain dropped (functional + dispatch variants kept).
- [x] Migrated the remaining examples to functional-first so they match the docs: `examples/greeter`,
      `examples/chat`, `examples/cluster`, `examples/k8s-silo`, and the `FleetAggregator` of
      `examples/thermostat` are now `defineGrain` factories. `ThermostatGrain` is kept deliberately as
      the `@grain()` class — the living interop example, consumed by the functional aggregator over
      the telemetry stream. Smoke tests stay green.
- [ ] Optional ergonomics from the review: move the facet hooks onto `ctx`
      (`ctx.persistentState(...)` / `ctx.reducerState(...)`) to drop the `INSTANCE`-symbol smuggling
      and the misleading `use*` prefix; add `useReminder` / `useTimer` / `useActivate` sugar.

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
- [x] Gateway discovery + failover — the client no longer pins one `gateway`; a `GatewayListProvider`
      (`staticGatewayProvider` / `membershipGatewayProvider` = active silos / `urlGatewayProvider`
      = `gateway: { url }`) feeds a `GatewayManager` (round-robin selection; `markAsDead` drops an
      unreachable gateway until the next `refresh` re-learns it — Orleans' `IGatewayListProvider` +
      `GatewayManager`). `ClientNode.invoke` fails over on a transport failure (connect/send throws or
      the reply times out): it drops that gateway and retries the next, refreshing the list once when
      exhausted; an error carried in a *response* (grain throw / gateway rejection) propagates without
      failover. `gateway` kept as one-entry shorthand. Tests: manager/provider units + two in-process
      e2e (skips an unreachable gateway and routes through a live one; discovers gateways from
      membership).
- [x] WebSocket client e2e — the client runs over the real WebSocket transport: a call routes
      client → gateway → activation and the reply flows back over a reverse connection to the client's
      own listener (`gateway-discovery.test.ts`, membership-discovered gateway). This surfaced and fixed
      a transport teardown bug: `WebSocketTransport.connect`'s `close()` waited on a `close` event that
      never fires when the peer already closed the socket — now it resolves immediately if the socket is
      already `CLOSED`, so client/silo shutdown can't hang regardless of close order.

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
- [x] `examples/migration` — live grain migration: a cart grain on silo-0 accumulates state (one item
      persisted, one only in memory), requests `moveTo(silo-1)`, and after the idle sweep serves from
      silo-1 with **both** items intact — proving the unflushed item rode the migration bag, not a
      storage re-read. Functional-first (`defineGrain` + `usePersistentState`, which auto-participates
      in migration); runnable via `pnpm --filter @tsva/example-migration start`.
- [x] `examples/broadcast` — broadcast-channel pub/sub fan-out ([ADR 0015](docs/adr/0015-broadcast-channels.md)):
      an alert publisher writes to a region channel `(alerts, region)`; a `RegionMonitor` and an
      `AuditLog` (two grain types implicitly subscribed to `alerts`, keyed by region) both receive each
      publish, with key-based isolation (eu alerts never reach us). Functional-first (`defineGrain` +
      `BROADCAST_CHANNEL_OBSERVER`); runnable via `pnpm --filter @tsva/example-broadcast start`.

## Beyond parity — browser state replication ([ADR 0017](docs/adr/0017-browser-state-replication.md))

- [x] ADR 0017 — design for replicating grain state to the browser and running permitted grains
      client-side under a server-enforced trust model. Settled: latency-first motivation; v1 is a
      server-authoritative **read-only** live read-view over the existing client→gateway WebSocket
      path; writable/optimistic/CRDT client state and browser-hosted grains deferred to follow-up
      ADRs. Status: Proposed (design only).
- [ ] Slice 1 — client-session identity + gateway authorization seam: an authenticated client
      identity in the preamble/request context; a gateway incoming call filter ([ADR 0012](docs/adr/0012-grain-call-filters.md))
      enforces a default-deny replication policy. Failing test: replication of an unmarked grain type
      is rejected at the gateway.
- [ ] Slice 2 — grain-type eligibility marker: `browserReplication` on `GrainOptions`, recorded in the
      silo registry and read server-side only (a client claim cannot override it).
- [ ] Slice 3 — read-view subscription protocol over WebSocket (subscribe/snapshot/delta/resync) built
      on event streams ([docs/09](docs/09-event-streams.md)) + state version/etag ([docs/07](docs/07-persistence.md)):
      snapshot then deltas, gap→resync, eventual consistency with the authoritative activation.
- [ ] Slice 4 — subscription lifecycle + resource bounds: re-subscribe across server activation
      deactivation/migration; tear down on browser disconnect.
- [ ] (follow-up ADRs) Layer 2 browser-hosted grains; writable client state with an optimistic/CRDT
      reconciliation model + offline support.
