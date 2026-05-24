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
- [ ] Multi-silo reminder ownership from the ring + rebalance on view change (single-silo owns the
      whole ring today); Redis (default) + Postgres reminder tables (need real infra)

## Deferred (later phases)

- Phase 6 event streams. See [`docs/13`](docs/13-roadmap-and-phases.md).

## On approach to v1 completion

- [ ] **Executable** worked example `examples/thermostat` — real runnable entry point (not snippets)
      over in-memory providers + in-process/WebSocket transport, with a smoke run wired into the test
      suite so it can't rot. (Phase 6 exit criterion: thermostat runs end-to-end.)
- [ ] Reconcile `docs/11` (public API) and `README` with the shipped API; add getting-started/run docs
- [ ] Final docs accuracy pass before declaring v1 done
