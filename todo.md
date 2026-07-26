# todo

Outstanding work is tracked as [GitHub issues](https://github.com/dave-hillier-co/thresh/issues);
this file is the index. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)).

Issues #18–#37 were implemented and closed in the 2026-07-24 burn-down, including the follow-up
remainders their implementations initially left undone (storage cancellation signals, per-call
deadline API, keepalive unbind on deactivation, and full-facet `@readOnly` guard coverage).

## Beyond parity

- [ ] [#38](https://github.com/dave-hillier-co/thresh/issues/38) Browser state
      replication & browser-hosted grains (read-only live read-views first).

## Deferred

- [ ] [#39](https://github.com/dave-hillier-co/thresh/issues/39) Additional stream
      backings behind the existing interfaces —
      [`docs/stream-backings-postgres-kafka.md`](docs/stream-backings-postgres-kafka.md). Phase 0
      (shared provider core), Phase 1 (Postgres backing, `addPostgresStreams`) and Phase 2 (Kafka
      backing, `addKafkaStreams`) are done; Phase 3 (LISTEN/NOTIFY polish, consumer-lag gauge,
      worked examples) is optional and remains.

## Orleans test-suite port (parity suite)

The functional test suites of Orleans `v10.1.0` are ported 1:1 into `packages/parity`;
`pnpm parity:scorecard [--run]` reports the standing. The gap backlog is closed — the scorecard
shows 0 gap-tagged tests across all ten in-scope suites; every upstream test is ported (passing)
or excluded with a documented reason. Notes: Orleans has no separate Reminders test project at
v10.1.0, and upstream itself skips its golden-path transaction runner (dotnet/orleans#9553), so
transaction behaviour here remains covered by `packages/hosting`'s transactions-cluster tests.
