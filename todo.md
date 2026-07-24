# todo

Outstanding work is tracked as [GitHub issues](https://github.com/dave-hillier-co/ts-virtual-actors/issues);
this file is the index. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)).

Issues #18–#37 were implemented and closed in the 2026-07-24 burn-down; the remainders their
implementations explicitly left undone are listed below.

## Follow-ups from the issue burn-down

- [ ] **Ambient cancellation remainders (from #18)** — journal/transactional-storage provider
      signatures still lack a `signal` param, and `getGrain` proxy calls have no per-call deadline
      API (today only `Dispatcher.invoke`'s `opts` exposes `deadlineMs`/`signal`).
- [ ] **`@readOnly` guard breadth (from #25)** — the dev-mode mutation guard
      (`SiloConfig.readOnlyStateGuard`, default off) proxy-wraps `@persistentState` facets only;
      reducer, durable and transactional state facets are not yet covered.

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
