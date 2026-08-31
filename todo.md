# todo

Outstanding work is tracked as [GitHub issues](https://github.com/dave-hillier-co/thresh/issues);
this file is the index. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)).

Issues #18–#37 were implemented and closed in the 2026-07-24 burn-down, including the follow-up
remainders their implementations initially left undone (storage cancellation signals, per-call
deadline API, keepalive unbind on deactivation, and full-facet `@readOnly` guard coverage).

## Driven by the first production use case

[BeneDB](https://github.com/dave-hillier-co/spacedb) — a wire-compatible SpiceDB on Thresh,
ported from the Orleans implementation — is the first production consumer, and the gaps it hits
land here. Closed so far: custom-storage log consistency, custom placement strategies,
`raceAbort` for cancellation-as-clean-exit, a service dimension in the Postgres and Redis storage
keys, and the observer seam on a `WebSocketTransport`-hosted silo. [`docs/orleans-to-thresh-port.md`](docs/orleans-to-thresh-port.md)
is the mechanical Orleans→Thresh reference that port maintains.

The service dimension is an **upgrade break for Redis**, recorded here because nothing else does.
`RedisGrainStorage`'s key gained a service segment (`{keyPrefix}:{serviceId}:state:…`, was
`{keyPrefix}:state:…`) and Redis has no `ALTER`, so state written under the old shape is orphaned
rather than corrupted — unreferenced keys that will never expire. Taken deliberately: Thresh has no
production deployments, and leaving the segment out keeps two services sharing one Redis silently
colliding, which is the bug. Postgres is **not** breaking: `start()` migrates an existing table in
place, backfilling to `DEFAULT_SERVICE_ID`, and a silo that names no `serviceId` reads exactly that
literal — the two must agree or every pre-existing row goes invisible on the next restart, which is
the failure this migration exists to avoid.

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
