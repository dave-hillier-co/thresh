# todo

Outstanding work is tracked as [GitHub issues](https://github.com/dave-hillier-co/ts-virtual-actors/issues);
this file is the index. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)). Each issue body carries the verified
current-state evidence (what already exists, what is missing, file pointers).

## Parity gaps

- [ ] [#21](https://github.com/dave-hillier-co/ts-virtual-actors/issues/21) Versioned,
      registry-based serializer (versioning, surrogates, polymorphism, `Set`, cycle guard).
- [ ] [#22](https://github.com/dave-hillier-co/ts-virtual-actors/issues/22) Stream
      `IStreamFailureHandler` provider wiring, durable poison store & producer registration.
- [ ] [#23](https://github.com/dave-hillier-co/ts-virtual-actors/issues/23) Transaction TM
      confirmation-worker keepalive.
- [ ] [#26](https://github.com/dave-hillier-co/ts-virtual-actors/issues/26) Directory handoff
      ACK-delete loop, recovery retry & register version gate.
- [ ] [#31](https://github.com/dave-hillier-co/ts-virtual-actors/issues/31) Timers —
      callback-initiated `change()`/`dispose()` (`operationType`).
- [ ] Ambient cancellation follow-ups from #18 (core slice landed: `@tsva/core/abort`,
      `InvocationRequest.deadline`, `AbortSignal` threaded through `InvocationContext`,
      `Dispatcher`/`TurnScheduler`/`ActivationData`, `GrainRuntime.getCancellationSignal()`, and
      `GrainStorage`/`PersistentState`) — journal/transactional-storage provider signatures still
      lack a `signal` param, and `getGrain` proxy calls have no friendlier per-call deadline API
      (today only `Dispatcher.invoke`'s `opts` exposes `deadlineMs`/`signal`).
- [ ] No hosting-layer caller invokes `unbindTransactionalStates` on grain deactivation yet
      (`ClusterNode.onDeactivated` is not extensible for this), so a long-idle activation with an
      unresolved transactional record keeps its confirmation-worker timer running until the
      process exits (follow-up from #23).
- [x] **`@readOnly` runtime check** — opt-in dev-mode mutation guard (`SiloConfig.readOnlyStateGuard`,
      default off): proxy-wraps `@persistentState` fields for the duration of a `readOnly` turn and
      throws `ReadOnlyStateViolationError` on a write, in-place mutation, or `write()`/`clear()` call.
      Reducer/durable/transactional state facets are not covered yet (GAP-READONLY-ENFORCEMENT).
      Closes #25.

## Bugs

- [ ] [#27](https://github.com/dave-hillier-co/ts-virtual-actors/issues/27) TestCluster teardown
      race — transport closed before `deactivateAll` runs `onDeactivate`.
- [ ] [#28](https://github.com/dave-hillier-co/ts-virtual-actors/issues/28) Durable jobs —
      cross-silo scheduling never delivers when another silo owns the shard.

## Test infrastructure

- [ ] [#29](https://github.com/dave-hillier-co/ts-virtual-actors/issues/29) Prune the stale
      `GapTag` union (scorecard shows 0 gap-tagged tests).
- [ ] [#30](https://github.com/dave-hillier-co/ts-virtual-actors/issues/30) Migrate the seven
      ad-hoc `buildCluster` helpers onto `TestCluster`.

## Parity follow-ups (from in-flight slices)

- [ ] [#34](https://github.com/dave-hillier-co/ts-virtual-actors/issues/34) Reminders — plumb
      `ReminderOptions` (`minimumPeriod`) through the silo builder.
- [ ] [#35](https://github.com/dave-hillier-co/ts-virtual-actors/issues/35) Directory —
      silo-liveness gate on `register` (Orleans `RegisterCore` parity).
- [ ] [#36](https://github.com/dave-hillier-co/ts-virtual-actors/issues/36) Client — per-attempt
      deadline so `callTimeoutMs` bounds total wall-clock across gateway failover.
- [ ] [#37](https://github.com/dave-hillier-co/ts-virtual-actors/issues/37) Durable jobs —
      in-grain `RunId` dedup on the delivery path.

## Beyond parity

- [ ] [#38](https://github.com/dave-hillier-co/ts-virtual-actors/issues/38) Browser state
      replication & browser-hosted grains (read-only live read-views first).

## Deferred

- [ ] [#39](https://github.com/dave-hillier-co/ts-virtual-actors/issues/39) Additional stream
      backings behind the existing interfaces.

## Orleans test-suite port (parity suite)

The functional test suites of Orleans `v10.1.0` are ported 1:1 into `packages/parity`;
`pnpm parity:scorecard [--run]` reports the standing. The gap backlog is closed — the scorecard
shows 0 gap-tagged tests across all ten in-scope suites; every upstream test is ported (passing)
or excluded with a documented reason. Notes: Orleans has no separate Reminders test project at
v10.1.0, and upstream itself skips its golden-path transaction runner (dotnet/orleans#9553), so
transaction behaviour here remains covered by `packages/hosting`'s transactions-cluster tests.

Closed during the 2026-07-24 todo audit (verified done, no issue needed): activation
repartitioning (`packages/runtime/src/placement/repartitioning/`), stream filtering
(`StreamFilter` + `SiloBuilder.addStreamFilter`), stream/broadcast delivery through the incoming
call-filter pipeline, the removed request-context gap tags, the migration-rehydration
stale-directory bug (`FailRehydrationTest` passes — register happens after rehydrate), and
`getReminder`/`getReminders` on `GrainRuntime`.
