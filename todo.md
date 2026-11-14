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
keys, the observer seam on a `WebSocketTransport`-hosted silo, Postgres journal storage, storage
backends (the host names providers, Thresh picks the database), a conditional append that
never lets a later confirm apply an event its caller was told failed, and `TestCluster.migrate`
(Orleans `MigrateAsync`) for moving an activation onto a chosen silo before killing it. [`docs/orleans-to-thresh-port.md`](docs/orleans-to-thresh-port.md)
is the mechanical Orleans→Thresh reference that port maintains.

Storage layouts carry no migrations. Thresh has no production deployments, so a layout change (the
service dimension of #59/#64 in every durable store, the reminder table's `last_fired_at`) is a
clean break on both Postgres and Redis: state written under an older layout is not read.

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
- Orleans comparison review, 2026-09-23 — subsystem-by-subsystem read of Thresh against the
  `dotnet/orleans` source, excluding deliberate deviations and everything above. Filed as
  [#88–#120](https://github.com/dave-hillier-co/thresh/issues?q=is%3Aissue+88..120):
  - **Messaging/client:** duplicate execution on connection loss (#88), stale client gateways (#89),
    deadline not carried cross-silo (#90), no cache invalidation or hop limit on forward (#110),
    silent serializer corruption (#119).
  - **Activation lifecycle:** calls to deactivating/migrating activations fail rather than reroute
    (#91), timer ambient-context leak (#92), scheduler admission vs `MayInvokeRequest` (#104),
    fixed-rate timers (#105), idle-collection race (#106), stuck turns not recycled (#107),
    call-chain reentrancy default (#118), minor items (#120).
  - **Migration/shutdown:** failed migration wedges the grain (#93), journaled/durable state lost on
    migration (#94), shutdown orphans and ordering (#108), call-filter re-invoke (#116).
  - **Persistence/journaling:** `confirmEvents` loss (#95), version reset after compaction (#96),
    stale etag on a missing record (#109), JournaledGrain + `@durable*` sharing a log (#117).
  - **Streams:** fan-out stops at the first failing subscriber (#97), ~300ms retry budget (#98),
    Postgres visibility gap (#99), Kafka batch loss/no-op rewind (#100), no delivery deadline
    (#111), `startToken`/multi-subscription (#112), Kafka ownership race (#113).
  - **Reminders/durable jobs:** `setTimeout` overflow past ~24.8 days (#101), non-owner reminder
    update (#102), stranded job shards (#103), backoff from poll start (#114), reminder catch-up
    drift (#115).
- [Design review — 2026-11-14](docs/project-review-2026-11-14.md) — a read for design issues rather
  than line-level bugs: responsibilities in the wrong place, invariants held by timing, mechanisms
  that should be one, and holes in the failure model. About 60 findings in nine groups, the most
  serious reproduced by probes against the production wiring (Kubernetes membership composed with
  the host and node) that no test in the suite exercises. Indexed below.
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

All filed as issues #67–#84 and fixed on `integration/2026-09-22-review-fixes`, since merged to `main`.
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
      reads a peer's pushed count, and every silo now pushes it on a `DeploymentLoadPublisher`-style
      timer (`loadPublishIntervalMs`, default 1s), so the activation-count inversion is gone.
      `ResourceOptimizedPlacement` still ignores its weights: the port has no environment-statistics
      provider (Orleans' `EnvironmentStatisticsProvider`, Kalman-filtered CPU and memory), so the
      load snapshot carries no CPU or memory signal to score with.
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

### Orleans comparison review (2026-09-23)

Fixed on `integration/2026-09-24-review-fixes`, each test-first and adversarially reviewed; the
full unit, parity and Postgres suites, typecheck and lint pass, and BeneDB typechecks and passes
against the integrated tree.

- [x] #88 connection loss is its own non-retriable `siloUnavailable` kind; only pre-execution
      rejections are resent.
- [x] #89 gateways drop disconnected clients after `clientDropTimeoutMs` and reject their pending
      calls; the client directory republishes on membership change and a refresh timer. Deliberate
      difference: calls to a disconnected-but-not-yet-dropped client are rejected at once rather than
      buffered.
- [x] #90 every request carries a relative time-to-live on the wire (re-based on the receiver's
      clock) and is dropped at turn start once expired; `callTimeout` is now configurable on
      `SiloConfig` / `TestClusterOptions`.
- [x] #91 calls reaching a deactivating or migrating activation are held and rerouted.
- [x] #92, #105 grain-timer ticks start with a clean ambient context and are fixed-delay.
- [x] #93 a failed migration still deactivates. #106 idle collection deletes only its own entry.
- [x] #94, #95, #96, #117 journals replay on rehydrate, `confirmEvents` keeps unappended events,
      the snapshot records the version, one state-machine manager per grain.
- [x] #98, #111 pulling-stream delivery retries until `maxEventDeliveryTimeMs` (1 min) and each
      delivery is bounded by the response timeout.
- [x] #100, #113 Kafka keeps an interrupted batch, rewind re-seeks, and partition acquire re-checks
      ownership and retries.
- [x] #101 `setTimer` chains delays beyond 2^31−1 ms.
- [x] #102, #103, #114, #115 reminders replace on etag change and stay on the `startAt + n·period`
      grid; durable jobs back off from completion and run a periodic shard check (10 min).
- [x] #104 turn admission mirrors `MayInvokeRequest` (blocking request, not "anything running").
- [x] #107 a stuck activation is deactivated and its queued calls rerouted. `MaxRequestProcessingTime`
      now defaults to Orleans' 2 hours (was 30s, below the one-minute call timeout).
- [x] #109 a non-empty etag against a missing record is a conflict in all three providers; `read()`
      of a missing record resets the value. #119 typed arrays and `-0` round-trip; unrepresentable
      values throw `UnsupportedValueError`.
- [x] #116 a re-invoking call filter re-runs the inner chain. #120 all five minor items.
- [ ] #74 **partly fixed** — a periodic load publisher now runs in production. `ResourceOptimizedPlacement`
      still scores by activation count only; it needs a CPU/memory statistics source.
- [ ] #97 **partly fixed** — a failing subscriber no longer takes the event from the others, but
      without a cursor per consumer it still holds back later events on that queue for up to
      `maxEventDeliveryTimeMs`.
- [x] #85 a clustered silo refuses a per-silo in-memory reminder table, grain store or journal store,
      and a multi-silo static view with no liveness, at build. `allowSharedInMemoryState()` admits
      in-memory instances shared in-process (`TestCluster`); `useStaticMembership`'s
      `allowWithoutLiveness` admits a deliberately static view.
- [ ] #85 follow-up: the same per-silo-in-memory hazard, unguarded, in the transactional-storage
      default (a per-silo `MemoryTransactionalStorage` unless `disableTransactions()`),
      `useMemoryDurableJobs()` and `useMemoryStreams()` with no argument.
- [x] #118 call-chain reentrancy is opt-in, as in Orleans: `RequestContext.allowCallChainReentrancy`
      / `suppressCallChainReentrancy`; a non-reentrant grain's callback into itself deadlocks until
      its response timeout. The parity port of `ConsistencyTestGrain` had dropped upstream's
      `[Reentrant]` and relied on the old always-on behaviour; it is reentrant again.
- [x] #87 interleaving is a documented contract (`docs/guide/grains.md#interleaving`), pinned by
      `packages/runtime/src/interleaving.test.ts`: an interleaved call runs only at another turn's
      `await`, never concurrently, and sees that turn's writes so far — hiding intermediate state is
      the grain's job (a confirmed view, as `JournaledGrain.state` is).
- [x] #129 a deep turn queue drains in linear time: an arrival is judged alone rather than
      rescanning the queue, and `pump` stops once nothing queued can be admitted. The
      `DuplicateActivations` parity stress test (10k calls queued per activation) passes again.
- [ ] #129 follow-up: `MsgpackSerializer` builds a new msgpack `Encoder` per message (a tenth of
      the stress test's CPU); a reused encoder would cut per-call cost. The parity project is not
      run in CI, which is how the regression went unnoticed.
- [ ] Needs a design decision, not attempted: #99 (Postgres stream visibility — serialise appends or
      a snapshot watermark), #112 (`startToken` / multiple subscriptions: implement or record as a
      deviation).

Follow-ups surfaced while fixing them:

- [ ] `ClientNode.invoke` re-sends to another gateway after a reply timeout — the client-side twin of #88.
- [ ] A silo declared dead by membership does not fail its pending calls unless the connection closes
      (Orleans `CallbackData.OnTargetSiloFail`); `InProcessTransport` never reports a lost connection.
- [ ] `LocationCache` is unbounded with no TTL/LRU (side remark in #110).
- [ ] `DistributedDispatcher` resends on any stale-kind rejection, so a call whose own body threw a
      nested `noActivation` could run twice; `LocalDispatcher` was narrowed to `isRerouteRejection`.
- [ ] A remote caller whose route came from a fresh directory lookup (not the cache) is not resent
      after a stuck or migrating activation rejects it.
- [ ] A held call (#91) is not cancelled by its caller's signal or deadline, only by
      `DeactivationTimeout`.
- [ ] A failed migration leaves the old directory entry in place (Orleans unregisters); the next call
      repairs it.
- [ ] `stopBudgetMs` covers grace plus `deactivateAll` only; `onBeforeDeactivate` and `onStop` run on top.
- [ ] Local calls still pass unrepresentable values (RegExp, sparse arrays) that a remote call rejects.
- [ ] `ClusterNode.deliverStreamEvent` and the memory provider's implicit fan-out have no delivery
      deadline of their own.
- [ ] `shardActivationBufferMs` is resolved but unused (Thresh shards have no start time distinct from
      their jobs' due times).

### Design review (2026-11-14)

Every finding's evidence and direction is in
[`docs/project-review-2026-11-14.md`](docs/project-review-2026-11-14.md); the ids below are its
headings. Not filed as issues. Fixes land on `integration/2026-11-14-design-review-fixes`, each
carrying a test that was observed failing first. Redis-, Postgres- and Kafka-backed paths were not
executed locally in either wave: the Redis job-shard scripts and keys changed and must run in CI.

Membership and partition ownership:

- [x] **M1** view changes are delivered by a one-shot wake-up to a loop that awaits store I/O.
      `updates()` has latest-value semantics and the host reconciles to the level.
- [x] **M2** a silo's ring is frozen at construction; under Kubernetes it starts empty. The node
      applies the current view in `start()`; ownership hooks run from one path.
- [x] **M3** join recovery runs with the read gate open (duplicate activations on join). An
      unknown previous owner now waits on every source, and the pull names the requester's ring.
- [ ] **M4** **partly fixed.** A removed silo is cleared from the cache, the pool and the client
      directory, and a peer that cannot be reached fails with the transports' one `ConnectFailure`
      (the cached route is dropped). Still open: calls to a silo that is dead but still listed
      keep failing until Kubernetes removes it (option C above).
- [x] **M5** per-silo view versions break routine routing (the reproduced half of #72). The
      Kubernetes view version is the highest EndpointSlice `resourceVersion` observed. Residuals:
      a watch that reports no revision falls back to a per-silo counter; a silo that listed after
      a slice deletion can differ from one that saw the event until the next slice write; and a
      not-ready joiner still counts itself in its own ring (the open half of #72).
- [ ] **M6** the directory handoff fails open and has no range or version unit
- [ ] **M7** **partly fixed.** A graceful release resets a job shard's adoption count, poisoning
      is logged and counted, and job ownership follows the node's applied view. Still open: the
      three services have no shared ownership abstraction and no fencing epoch on their writes.
- [x] **M8** health signals do not measure what they name. `membershipHealthy` follows the
      applied view, `overloaded` follows the load-shedding detector, `transportReady` follows the
      listener, and liveness can fail after sustained self-probe failure (opt-in). Residual: a
      listener `error` event still leaves `transportReady` true.
- [ ] **M9** **partly fixed.** A dial is bounded at 5 s. Still open: replies use a
      reverse-dialled connection, and responses match by id alone with no process epoch.

Transactions:

- [ ] **T1** the caller-side agent drives 2PC; the TM is only a commit-record register
- [x] **T2** `prepare` validates nothing (a re-activated participant loses its write). `prepare`
      carries the access counts and votes no unless the lock is still held with them.
- [x] **T3** `status(): boolean` conflates "not yet decided" with "aborted". Status is
      committed / undetermined / presumed abort.
- [ ] **T4** **partly fixed.** A holder past `lockTimeoutMs` (8 s) is broken, waiters use
      `lockAcquireTimeoutMs` (10 s), protocol calls are always-interleave, no timer outlives its
      resource, and the options are settable through the builder and `TestCluster`. Still open:
      commit records are never collected (needs T1's Confirm phase), and the prepared-participant
      ping is a stand-in.
- [ ] **T5** pending records are siblings but the storage contract encodes a chain (root of #78)
- [x] **T6** nothing blocks deactivation while a lock or prepared record is live. Deactivation
      waits for the block on every path, bounded at Orleans' 5 s.

Activation lifecycle:

- [x] **L1** deactivation has no single owner. One joinable pipeline on the activation, in
      Orleans' order, used by every path; no invalid entry waits in the catalog for a sweep.
- [ ] **L2** lifecycle operations are ordinary turns; a deactivation request is a polled flag
- [ ] **L3** a 5-value state plus about 13 flags; admission is decided in about ten places
- [ ] **L4** directory registration lives outside the activation; cold-grain order rests on timing
- [x] **L5** the placement pipeline exists twice. Migration targets come from the same pipeline
      as new activations; directed movers pass a hint; stateless workers and system grains never
      migrate.
- [x] **L6** the only environment-statistics source is a test stub, with public latches. An
      injectable provider with a Node default feeds load shedding and placement; the latches
      work only on a silo built with test hooks. Resource-optimized scoring uses its weights but
      has no Kalman filtering or candidate subset yet (the open part of #74).
- [x] **L7** idle collection is a sequential full-map sweep. Stale activations are selected in
      one pass and deactivated with bounded concurrency; sweeps never overlap.
- [ ] **L8** a same-silo call has no default deadline

Wire surface and location transparency:

- [x] **W1** dispatch by bare method name: no declared wire surface. Only a type's declared
      methods dispatch, references keep JavaScript protocol names local, and a `JournaledGrain`'s
      framework hooks are excluded.
- [x] **W2** the value codec reserves keys inside user data. Objects owning `$thresh` or a
      dangerous key travel in a `plainObject` envelope and round-trip as themselves.
- [ ] **W3** **partly fixed.** `wireSemanticsForLocalCalls` (silo config and `TestCluster`)
      round-trips same-silo calls through the codec; it is off by default. Forced on, 11 tests
      fail: 7 pass values the wire refuses, 3 rely on aliasing, 1 pins local-only semantics.
- [ ] **W4** no compatibility contract for the wire or stored state
- [x] **W5** a decode failure is a process fault. An undecodable frame closes that connection
      and is counted; one failure policy wraps every system handler. Residual: a frame that
      decodes to a non-message object is delivered to the handler, whose failure is contained.
- [ ] **W6** "safe to resend" has three definitions
- [x] **W7** the error taxonomy lives in parallel tables that have drifted. One table, a test
      that enumerates every exported error, and `isThreshRuntimeError` classifies as Orleans does.
- [ ] **W8** `GrainId` has two equalities

Verification model:

- [ ] **V1** **partly fixed.** `TestCluster` can run every silo on its own Kubernetes
      membership over a shared fake API, with held and released views, and a kill is now a
      crash. The default is still the shared static membership.
- [ ] **V2** **partly fixed.** A shared memory stream provider binds per silo and delivers
      through a live one. The memory and pulling providers are still different models.
- [ ] **V3** three host layers with different defaults
- [ ] **V4** the parity metric covers neither the largest deviation nor functional grains
- [x] **V5** no fault-injection seam. A fault-injecting transport (drop, delay, fail connects,
      partition) and storage decorators ship in the harness.

State and journaling:

- [x] **S1** the journal adaptor swallows the lost-writer signal and stays stale. A lost
      append resyncs; only the conditional entries are dropped; a lost unconditional confirm is
      retried on top of the refreshed view.
- [ ] **S2** one storage write per durable-collection mutation, no atomic unit
- [x] **S3** `JournaledGrain` event history is truncated by the shared compaction threshold.
      Truncation is the documented default and whole-history retention is opt-in. Opting in
      rewrites the history at every compaction until the storage seam can snapshot without
      truncating.
- [x] **S4** log sharing is inferred by object identity over a constant log name. Sharing is
      decided by a storage's `backingKey`.
- [ ] **S5** no shared facet contract
- [x] **S6** the in-memory grain store is not a model of the durable ones. It round-trips state
      through the value codec.
- [ ] **S7** **partly fixed.** A transactional state uses the storage provider it names. Still
      open: the storage backend does not cover transactional storage or job shards, and neither
      has a Postgres store.
- [ ] **S8** `defineReducerGrain` effects are neither atomic nor durable

Streams, reminders and durable jobs:

- [ ] **B1** pulling-stream delivery state is one cursor per physical queue (root of #97, #112)
- [ ] **B2** retention and rewind are not part of the queue seam
- [ ] **B3** **partly fixed.** The owner re-reads its shard, so a lost forward no longer strands
      a job; failures are logged; Redis keys carry the service id. Shards are still cluster-wide
      time buckets.
- [ ] **B4** **partly fixed.** A never-fired reminder found past its due time fires promptly, so
      the first tick is late by at most one refresh, not one period. Registration is still not
      routed to the range owner as Orleans does.
- [ ] **B5** the `PullableQueue` contract assumes token order equals visibility order (root of #99)
- [ ] **B6** Kafka holds no authoritative assignment or offsets

Composition, configuration and operability:

- [ ] **C1** **partly fixed.** An unknown stream provider name and a filter that would be
      ignored are refused. The builder still creates resources at call time and validates by
      special case.
- [ ] **C2** options live in four hand-synchronised layers
- [ ] **C3** lifecycle is hook arrays, not a staged lifecycle
- [x] **C4** the logger is not a seam and the runtime is silent by default. One `logger` on the
      silo configuration reaches the node, dispatchers, reminders, jobs, the endpoint watch, the
      self-probe and the WebSocket transport; the default writes warn and error to the console.
      Residual: the Redis, Postgres and Kafka stream providers and grain services do not take it.
- [ ] **C5** `ClusterNode` stands in for a missing system-target abstraction
- [ ] **C6** `GrainRuntime` is a god-interface
- [ ] **C7** the silo builder statically imports every backing driver

API boundary and consumer coupling:

- [ ] **A1** nothing separates public API from internals
- [ ] **A2** nothing runs BeneDB when Thresh changes
- [ ] **A3** two authoring models are not in lockstep

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
`pnpm parity:scorecard [--run]` reports the standing: **571 ported / 0 gap / 741 excluded** over
ten in-scope suites (1312 declarations, every one accounted for). Earlier citations of
502 / 0 / 475 — including the 2026-08-04 review's parity row — came from the scorecard's old
regex parser, which could only see declarations whose first argument was an inline literal;
267 exclusions written as `orleansTest.excluded(REASON, …)` and 43 ports named by template
literal were counted in no column at all. The parser now reads the TypeScript AST and fails the
run when the accounted total does not reconcile with the call sites it found. Notes: Orleans has
no separate Reminders test project at v10.1.0, and upstream itself skips its golden-path
transaction runner (dotnet/orleans#9553), so transaction behaviour here remains covered by
`packages/hosting`'s transactions-cluster tests.

- [ ] Port `test/Orleans.Runtime.Internal.Tests/MessageScheduling` (`AllowCallChainReentrancyTests`,
      `DisabledCallChainReentrancyTests`, `ReentrancyTests`) and their grains (`DeadlockGrain`,
      `CallChainReentrancyGrain`, `NonReentrantGrain`, the `MayInterleave` grains). No parity suite
      mirrors that project yet; `packages/runtime/src/reentrancy.test.ts` covers the same cases.
- [ ] 21 ported declarations take their id from a runtime value (`for (const testClass of [...])` in
      the two cancellation-token suites), so the scorecard counts them but cannot match them to a
      vitest title — `--run` now fails on exactly that. Give each an id it can resolve (name the
      fixture classes literally, or teach the parser to enumerate a loop over string literals).
