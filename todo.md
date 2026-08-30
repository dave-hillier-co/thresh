# todo

Outstanding work is tracked as [GitHub issues](https://github.com/dave-hillier-co/thresh/issues);
this file is the index. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)).

Issues #18–#37 were implemented and closed in the 2026-07-24 burn-down, including the follow-up
remainders their implementations initially left undone (storage cancellation signals, per-call
deadline API, keepalive unbind on deactivation, and full-facet `@readOnly` guard coverage).

## Driven by the first production use case

[SpaceDB](https://github.com/dave-hillier-co/spacedb) — a wire-compatible SpiceDB on Thresh,
ported from the Orleans implementation — is the first production consumer, and the gaps it hits
land here. Closed so far: custom-storage log consistency, custom placement strategies, and
`raceAbort` for cancellation-as-clean-exit. [`docs/orleans-to-thresh-port.md`](docs/orleans-to-thresh-port.md)
is the mechanical Orleans→Thresh reference that port maintains.

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

- [ ] [#55](https://github.com/dave-hillier-co/thresh/issues/55) follow-on: make the observer
      seam work on a `WebSocketTransport`-hosted silo. `requireObserverHosting()` now fails such a
      silo at `build()` instead of at the first `createObjectReference` call, but the underlying
      restriction stands: `ClientNode.connect()` listens on its own endpoint and the silo dials it
      back, so a WS-hosted embedded client needs a real second listening port and a reachable
      advertised address that `SiloConfig` does not supply. Two ways out, both deferred: make the
      client leg duplex over its own outbound connection (Orleans' shape — `WebSocketTransport`'s
      socket is send-only today, its message handler consumed by `awaitAck`), or auto-provision an
      ephemeral port and advertise it. See [`docs/deviations.md`](docs/deviations.md).

## Orleans test-suite port (parity suite)

The functional test suites of Orleans `v10.1.0` are ported 1:1 into `packages/parity`;
`pnpm parity:scorecard [--run]` reports the standing. The gap backlog is closed — the scorecard
shows 0 gap-tagged tests across all ten in-scope suites; every upstream test is ported (passing)
or excluded with a documented reason. Notes: Orleans has no separate Reminders test project at
v10.1.0, and upstream itself skips its golden-path transaction runner (dotnet/orleans#9553), so
transaction behaviour here remains covered by `packages/hosting`'s transactions-cluster tests.
