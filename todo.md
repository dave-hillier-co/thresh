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
- [Project review — 2026-09-22](docs/project-review-2026-09-22.md) — the first review to actually run
  the suites, the scorecard and the examples. It confirms the runtime and the test suite hold up
  (the suite is genuinely sociable: zero `vi.mock` across 221 files), and registers 22 open findings
  F1–F22 across the directory failure paths, placement input, transactions, the parity metric and
  documentation accuracy. The packaging/release question is out of scope by request.
- The 2026-09-02 correctness review's findings were fixed in-tree (transaction lock release /
  in-doubt `recordCommit`, transport `'error'` handling and per-peer fast-fail, monotonic stream
  cursors, drained durable-job stop, reminder `lastFiredAt`, codec prototype-pollution guard,
  non-zero drain grace, EndpointSlice list→watch `resourceVersion`, CI workflow). The rest closed
  on 2026-09-03: call-filter `undefined` short-circuit, EndpointSlice reconnect backoff+jitter,
  dead recovery-version gate removal, wait-die (timestamp, id) tie-break, the Postgres
  migration-race flake (`42704`/`42P16`), the stale captured ring in `beginRecovery`, and `oneWay`
  locality. That backlog was empty; the 2026-09-22 review reopened it — see the findings index below.

### Follow-ups surfaced while closing it

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
      puller. With the precise ACK this is merely wasteful for the puller — but it also means a
      pull can return an entry the source did not own when the pull was issued, which is the one
      case `awaitRecovered`'s per-source gate cannot wait for (costing a miss that lazy activation
      rebuilds rather than a wrong answer). Filtering needs `message.sendingSilo` threaded into
      `applyDirectoryOp`.
- [ ] Per-silo override of a **decorator-declared** `collectionAgeSeconds` is still impossible
      (only the process-wide grain metadata can change it, which by construction cannot differ
      between two silos in one process). Out of scope for #66, which is closed; needs its own issue
      if BeneDB's grain classes declare their own ages.
- [ ] `docs/deviations.md` needs the one-line note that local peer suspicion (option C, item 9) is
      placement-only and never a membership status.
- [ ] Option C is designed but not built — land it as the two slices the design note names
      (stage 1: sensor + metrics; stage 2: fail-fast + placement suppression).

### Findings from the 2026-09-22 review

All filed as issues #67–#84 and fixed on `integration/2026-09-22-review-fixes` (**not yet on `main`**).
Every fix carries a test that was run and observed failing first; the full suite, typecheck, lint and
all six runnable examples pass on the merged branch. Every finding's evidence, repro and severity is
in [`docs/project-review-2026-09-22.md`](docs/project-review-2026-09-22.md).

- [x] [#67](https://github.com/dave-hillier-co/thresh/issues/67) **F1** — a draining silo's directory
      entries were deleted while it was still serving. Fixed by separating readiness from liveness:
      `KubernetesMembership` now yields `active`/`draining`, and only endpoint *removal* drops entries.
- [x] [#68](https://github.com/dave-hillier-co/thresh/issues/68) **F2** — recovery runs on every view
      change that grants a range, and a re-acquired range is adopted back from the silo's own retained
      snapshot. Supersedes the first follow-up above.
- [x] [#69](https://github.com/dave-hillier-co/thresh/issues/69) **F3** — the recovery ACK now carries
      the full `GrainAddress` and deletes only on an exact match. A narrower case remains (a
      re-retained identical address can still be deleted by a very late ACK); closing it needs a
      per-entry version through the pull payload.
- [x] [#70](https://github.com/dave-hillier-co/thresh/issues/70) **F4** — the membership watch logs and
      continues instead of dying. The trigger in the original report was **wrong** and is corrected in
      the review doc and on the issue.
- [x] [#71](https://github.com/dave-hillier-co/thresh/issues/71) **F5** — ownership is re-checked after
      the wait on both the local and remote paths.
- [ ] [#72](https://github.com/dave-hillier-co/thresh/issues/72) **F6** — **open, needs a design
      decision.** Documented and pinned, not fixed: a cluster-wide version needs a shared ordered view
      identity plus a rolling-upgrade story, and "stop injecting self" would defeat the self-probe and
      break local placement. Coupled to #67. See the issue's decision comment.
- [x] [#73](https://github.com/dave-hillier-co/thresh/issues/73) **F7** — exhausted recovery is logged,
      counted (`thresh.directory.recovery.*`), re-armed on a jittered backoff, and gated per source
      rather than globally. The per-source gate is an approximation: `serveRecover` is still unfiltered
      by requester, so a source can return an entry it did not own, costing a miss that lazy activation
      rebuilds (never a wrong answer).
- [ ] [#74](https://github.com/dave-hillier-co/thresh/issues/74) **F8** — **partly fixed.** Placement
      now reads a peer's pushed count, but `publishLoadStats` is only ever called from the test-only
      load-shedding hooks, so nothing publishes in production and the inversion persists. Needs a
      periodic publisher. `ResourceOptimizedPlacement` still ignores its weights — there is no CPU or
      memory signal to score with. See the issue's comment.
- [x] [#75](https://github.com/dave-hillier-co/thresh/issues/75) **F9** — a wire-arrived stateless-worker
      call now joins the receiving silo's local pool instead of being directory-registered.
- [x] [#76](https://github.com/dave-hillier-co/thresh/issues/76) **F10** — `release` settles the
      waiters it removes, exactly as the deadline path already did.
- [x] [#77](https://github.com/dave-hillier-co/thresh/issues/77) **F11** — a late enlistment is now
      refused with `TransactionAlreadyResolvedError`, before it takes a lock. **Behaviour change:** a
      detached `oneWay` + `supported` callee that previously lost its write silently now throws into
      application code. BeneDB should know before this lands.
- [ ] [#78](https://github.com/dave-hillier-co/thresh/issues/78) **F12** — **partly fixed** (`Refs`,
      not `Closes`). The sequence-id allocation and three neighbouring paths are fixed. Two residuals
      need a design decision: a live `commit` still promotes up to its id and can sweep in an
      unresolved record below it, and a resolution's abort still drops everything above its own id.
      Both are inexpressible with `store`'s range deltas — dropping one record while keeping a newer
      one is one non-atomic call apart.
- [x] [#79](https://github.com/dave-hillier-co/thresh/issues/79) **F13** — the commit record is written
      durably before `this.metadata` adopts it.
- [ ] [#80](https://github.com/dave-hillier-co/thresh/issues/80) **F14** — **fixed locally, one hole
      remains.** `TransactionParticipant`/`TransactionManager` are now split so a committer can never
      be elected — but `SerializedParticipant` carries no role, so a participant merged back from
      another silo is taken as manager-capable and a *remote* committer could still be elected.
      Closing it needs a role or status field on the transaction header.
- [x] [#81](https://github.com/dave-hillier-co/thresh/issues/81) **F15** — a failed resolution no longer
      kills the worker; the record stays queued and is retried on the backoff.
- [x] [#82](https://github.com/dave-hillier-co/thresh/issues/82) **F16** — an abandoned call's deadline
      no longer escalates to an unhandled rejection, sends that throw release their entry, and
      shutdown settles outstanding calls. `examples/migration` exits 0.
- [x] [#83](https://github.com/dave-hillier-co/thresh/issues/83) **F17** — `decodeValue` now reads the
      stamp and refuses a version this build cannot decode, on tags whose shape it knows. This makes
      `EPICS.md`'s "versioned serializer" claim true. Note the tradeoff: a future wire-shape bump will
      be *refused* by older readers rather than silently degraded.
- [x] [#84](https://github.com/dave-hillier-co/thresh/issues/84) **F18** — the scorecard walks the
      TypeScript AST, counts every call site, and fails the run when they do not reconcile. Corrected
      totals: **570 ported / 0 gap / 742 excluded** (was 502/0/475 — the parser could only see inline
      literals). One new follow-up below.

Not filed — test infrastructure and documentation rather than bugs:

- [ ] **F19** "0 gap" was reached partly by reclassifying 67 gap-tagged tests as excluded; `excluded`
      is a no-op and `--run` never asserts `pass + fail === ported`.
- [ ] **F20** The parity suite never runs in CI (67s to add).
- [ ] **F21** `runtime/grain-directory-tests.test.ts` asserts against an in-file shim and counts as 5
      ported tests, on a justification that is now stale.
- [ ] **F22** Redis- and Kafka-backed tests vanish silently when the service is absent (~176 skipped locally).
- [ ] The 16 documentation corrections tabled in the review — docs contradicting the code in
      `deviations.md`, `orleans-to-thresh-port.md`, `EPICS.md` and `README.md`.

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
`pnpm parity:scorecard [--run]` reports the standing: **570 ported / 0 gap / 742 excluded** over
ten in-scope suites (1312 declarations, every one accounted for). Earlier citations of
502 / 0 / 475 — including the 2026-08-04 review's parity row — came from the scorecard's old
regex parser, which could only see declarations whose first argument was an inline literal;
267 exclusions written as `orleansTest.excluded(REASON, …)` and 43 ports named by template
literal were counted in no column at all. The parser now reads the TypeScript AST and fails the
run when the accounted total does not reconcile with the call sites it found. Notes: Orleans has
no separate Reminders test project at v10.1.0, and upstream itself skips its golden-path
transaction runner (dotnet/orleans#9553), so transaction behaviour here remains covered by
`packages/hosting`'s transactions-cluster tests.

- [ ] 21 ported declarations take their id from a runtime value (`for (const testClass of [...])` in
      the two cancellation-token suites), so the scorecard counts them but cannot match them to a
      vitest title — `--run` now fails on exactly that. Give each an id it can resolve (name the
      fixture classes literally, or teach the parser to enumerate a loop over string literals).
