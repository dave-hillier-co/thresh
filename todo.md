# todo

Outstanding work is tracked as [GitHub issues](https://github.com/dave-hillier-co/thresh/issues);
this file is the index. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)).

Issues #18–#37 were implemented and closed in the 2026-07-24 burn-down, including the follow-up
remainders their implementations initially left undone (storage cancellation signals, per-call
deadline API, keepalive unbind on deactivation, and full-facet `@readOnly` guard coverage).

## Driven by the first production use case

[BeneDB](https://github.com/dave-hillier-co/benedb) — a wire-compatible SpiceDB on Thresh,
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

#64 carried the same dimension to every other durable store that only partitioned by table name or
key prefix (`PostgresReminderTable`, `RedisReminderTable`, `RedisJournalStorage`,
`RedisTransactionalStorage`, the Postgres/Redis stream registries, cursor stores and queues), so the
Redis upgrade break above now also orphans: reminder index/grain/entry keys, journal logs and their
version counters, transactional records — including in-doubt (`PENDING`) ones, so **drain in-flight
transactions before upgrading** a Redis-backed transactional deployment — stream subscriptions,
stream cursors, and stream queue entries. A Redis-streams deployment should similarly **drain queues
before upgrading**: undelivered entries and committed cursors both orphan together (not just the
cursor), so there is no double-delivery risk — the stream simply restarts empty under the new key
shape. The Postgres stores in this list migrate in place the same way `PostgresGrainStorage` does, so
they carry no such break.

`PostgresStreamFailureStore` (append-only poison-delivery diagnostics, surrogate PK) was left out of
#64's scope: commingling failure records across services is cosmetic, not a correctness bug like the
stores above. Worth a service dimension eventually for tidiness, not urgently.
## Review

- [Project review — 2026-08-04](docs/project-review-2026-08-04.md) confirms the current stated aims are met, with only the explicitly beyond-parity browser work and deferred stream-backing polish left open.
- The 2026-09-02 correctness review's findings were fixed in-tree (transaction lock release /
  in-doubt `recordCommit`, transport `'error'` handling and per-peer fast-fail, monotonic stream
  cursors, drained durable-job stop, reminder `lastFiredAt`, codec prototype-pollution guard,
  non-zero drain grace, EndpointSlice list→watch `resourceVersion`, CI workflow). Still open from
  that review:
  - [ ] `oneWay` calls to a **local** activation await the whole callee turn while remote ones
        resolve on send (`packages/runtime/src/local-dispatcher.ts`, `distributed-dispatcher.ts` vs
        `cluster-node.ts`) — a location-transparency break; make local one-way calls detach.
  - [ ] Directory range recovery closes over the ring captured at `beginRecovery`
        (`packages/runtime/src/cluster-node.ts` `beginRecovery`); overlapping membership changes
        can register entries under a stale ring — re-check ownership against the live ring.
  - [x] Call-filter `undefined` short-circuit false positive, EndpointSlice reconnect
        backoff+jitter, dead recovery-version gate removal, wait-die (timestamp, id) tie-break,
        and the Postgres migration-race flake (`42704`/`42P16`) — fixed 2026-09-03.

## Beyond parity

- [ ] [#38](https://github.com/dave-hillier-co/thresh/issues/38) Browser state
      replication & browser-hosted grains (read-only live read-views first).

## Deferred

- [ ] Runtime key-kind assertion in `GrainFactory.getGrain` (assert the supplied key's kind matches
      `GrainInterface.key` where declared). Blocked on implicit stream subscriptions, which synthesise
      string keys for possibly integer-keyed grains
      (`packages/streams/src/implicit-subscriptions.ts`); fix that first or the check breaks implicit
      delivery. Type-level key kinds are already enforced, so this is defence in depth.
- [ ] Delete the residual no-op `extends GrainWithStringKey` from non-parity test fixtures. Pure
      deletion, no type change — the markers stay exported either way (as nominal names for
      `GrainKey<TKey>`), and `packages/parity` keeps them permanently for upstream traceability.
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
