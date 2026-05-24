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
- [ ] Slice 10: static membership + rebalancing — remove silo → ring rebalances, grains reactivate
- [ ] Slice 11: WebSocket transport — cross-silo over real sockets, preamble handshake, clusterId mismatch rejected

## Deferred (later phases)

- Phase 3 Kubernetes membership/hosting; Phase 4 persistence; Phase 5 timers/reminders;
  Phase 6 event streams. See [`docs/13`](docs/13-roadmap-and-phases.md).
