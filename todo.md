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
- [ ] Real `@kubernetes/client-node` EndpointSlice watch adapter (the `EndpointWatch` implementation)
- [x] Health endpoints (`/ready`, `/live`, `/startup`) — `HealthCheck` probe logic + `HealthServer`
- [x] Graceful drain — `GracefulShutdown` flips readiness then stops the node; `SIGTERM` handler
- [ ] Hosting builder (`createSilo().useKubernetesMembership()…`) tying node + health + drain together
- [ ] kind cluster e2e — 3-silo `StatefulSet`, pod-kill reactivation, rolling update (needs a live cluster)

## Deferred (later phases)

- Phase 4 persistence; Phase 5 timers/reminders; Phase 6 event streams.
  See [`docs/13`](docs/13-roadmap-and-phases.md).
