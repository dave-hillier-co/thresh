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
  non-zero drain grace, EndpointSlice list→watch `resourceVersion`, CI workflow). The rest closed
  on 2026-09-03: call-filter `undefined` short-circuit, EndpointSlice reconnect backoff+jitter,
  dead recovery-version gate removal, wait-die (timestamp, id) tie-break, the Postgres
  migration-race flake (`42704`/`42P16`), the stale captured ring in `beginRecovery`, and `oneWay`
  locality. The review backlog is empty.

### Follow-ups surfaced while closing it

- [ ] **An incumbent that gains a range never pulls it.** `beginRecovery` runs only on JOIN
      (`start()` and `updateView`'s `!wasActive` gate), so a silo that gains a range because
      another silo *left* never pulls: the previous owner's retained `handoffSnapshot` entries sit
      unpulled until `recoveryRetentionMs` expiry and then degrade to lazy rebuild. Orleans runs
      `AcquireRangeAsync` for the added range on every partition on every view change. Same family
      as the stale-ring bug and the largest remaining directory gap.
- [ ] A transactional writer that waits for a lock does so **inside an exclusive turn**, blocking
      the abort turns that would release the conflicting holders.
      `packages/parity/src/transactions/exclusive-lock-transaction-memory-tests.test.ts` only
      passes because cold-grain CAS losers currently jump ahead of the winner, so the youngest
      (immediately dying) transaction happens to run first. Any change that makes cold-grain
      admission order match call order for *awaited* calls will deadlock it.
- [ ] `ClusterNode.receiveRequest` awaits `dispatcher.deliverLocal` for inbound wire messages and
      discards a one-way failure silently — route it through the same catch-and-log as
      `dispatchDetachingOneWay`.
- [ ] `LocalDispatcher`'s logger is not wired from `SiloOptions` (there is no logger option there),
      so a `Silo`-hosted (non-`createSilo`) host logs detached one-way failures to `noopLogger`.
- [ ] `serveRecover()` is unfiltered by requester: it serves the whole `handoffSnapshot` to any
      puller. With the precise ACK this is merely wasteful, not incorrect; filtering needs
      `message.sendingSilo` threaded into `applyDirectoryOp`.
- [ ] Per-silo override of a **decorator-declared** `collectionAgeSeconds` is still impossible
      (only the process-wide grain metadata can change it, which by construction cannot differ
      between two silos in one process). Out of scope for #66, which is closed; needs its own issue
      if BeneDB's grain classes declare their own ages.
- [ ] `docs/deviations.md` needs the one-line note that local peer suspicion (option C, item 9) is
      placement-only and never a membership status.
- [ ] Option C is designed but not built — land it as the two slices the design note names
      (stage 1: sensor + metrics; stage 2: fail-fast + placement suppression).

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
