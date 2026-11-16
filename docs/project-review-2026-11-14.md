# Design review — 2026-11-14

Reviewed tree: `origin/main` at `575b63c` (2026-10-22). Scope: design issues, not line-level bugs —
responsibilities in the wrong place, invariants held by timing or convention, mechanisms that
should be one, seams that make correct behaviour inexpressible, and gaps in the failure model.
Items already tracked in `todo.md` are listed only where a deeper root cause was found.

Method: six subsystem reads (cluster routing, activation lifecycle, core API, state and
transactions, streams/reminders/jobs, hosting) plus a cross-cutting pass on the package graph and
the BeneDB coupling. Paths are repo-relative under `packages/` unless prefixed.

Evidence key:

- **[ran]** — a probe was executed against the tree and re-run during synthesis.
- **[ran once]** — a probe was executed by the subsystem reviewer; not re-run.
- **[read]** — established by reading the cited lines; no execution.
- **[unverified]** — reasoned from the code, not confirmed.

Probes used the in-process transport and a fake EndpointSlice watch unless stated; nothing was run
on a real Kubernetes cluster.

## The short version

1. Membership is a per-silo observation with lossy delivery, and four ownership mechanisms are
   built on it without fencing. The production membership path is not exercised by any test.
2. Transactions are not the Orleans distributed TM: the caller-side agent drives 2PC, `prepare`
   validates nothing, and nothing times out. A single participant re-activation loses a write in a
   transaction that reports committed.
3. The activation lifecycle has no single owner and no state machine.
4. There is no declared wire surface and no compatibility contract: any function-valued property
   is remotely callable, user data can collide with the codec's tag, and local calls skip the codec.
5. The test harness and the parity metric cannot see the failure paths, which is why each review
   round finds dozens more.

## 1. Membership and partition ownership

**M1. View changes are delivered by a one-shot wake-up to a loop that awaits store I/O.** [ran once]
`clustering-k8s/src/kubernetes-membership.ts:89-93,113-116` (same shape in
`runtime/src/static-membership.ts:44-48`); consumer at `hosting/src/silo-host.ts:321-328`. A waiter
exists only while the consumer is idle; the loop body awaits ownership hooks that do Postgres,
Redis and Kafka I/O. A change arriving mid-hook is never applied until some later event. Orleans'
feed has latest-value semantics (`src/Orleans.Core/Utils/AsyncEnumerable.cs`). Direction:
level-triggered reconcile inside the node — loop until applied version equals current.

**M2. A silo's ring is frozen at construction; under Kubernetes it starts empty.** [ran]
`runtime/src/cluster-node.ts:689-690` builds the ring when the node is constructed, before the
watch's first list. `clustering-k8s/src/kubernetes-endpoint-watch.ts:121-140` publishes that list
inside `start()`, before `silo-host.ts:269` registers a listener. `directory/src/distributed-grain-directory.ts:101-103`
rejects every route on an empty ring. Observed: after `host.start()` the joiner owned 0 ranges,
`ready()` was all-true, and every call failed with `staleView` until a second EndpointSlice event.
`addStartupTask` cannot call a grain under Kubernetes membership. In a real cluster the pod's own
readiness flip supplies the second event, so the window is bounded.

**M3. Join recovery runs with the read gate open.** [ran]
`cluster-node.ts:1262` guards start-time recovery on `appliedVersion > 1`, which only the test
fixture satisfies. When the first applied view arrives, `:1405-1407` passes an undefined previous
owner for an empty old ring and `:1522-1523` returns without waiting. Observed: 16 of 60 grains
live on the incumbent got a second activation on the joiner (active on both silos). Orleans locks
the added range before any await (`GrainDirectoryPartition.cs:418-476`).

**M4. A silo that leaves the Kubernetes way is never cleaned out of peers' routing state.** [ran]
`cluster-node.ts:1355-1361` iterates the previous *ring*; Kubernetes reports ready → not ready →
removed, so by the removal event the silo is no longer in the old ring and the loop never visits
it. The transports also disagree on "peer gone": the in-process fake throws a reroutable
`unknownTarget`, the WebSocket transport surfaces raw `ECONNREFUSED`
(`messaging/src/web-socket-transport.ts:143-150` vs `in-process-transport.ts:93-96`). Observed over
real sockets: 18 of 90 grains cached on a survivor still failed 5.5 s after removal; nothing else
invalidates those entries.

**M5. #72 is not only a design question — it breaks routine routing.** [ran]
Two silos with identical views but counters 4 and 2: 18 of 40 first calls through one and 12 of 40
through the other failed with `GrainCallTimeoutError`. Cause: `applyDirectoryOp` →
`awaitView(op.version)` (`cluster-node.ts:3323,1478-1495`) waits for a counter that never catches up.

**M6. The directory handoff fails open and has no range or version unit.** [read]
`cluster-node.ts:1519-1533,1603-1670,1727-1736`; `directory/src/local-directory-partition.ts:11-18`.
Handoff state is a flat per-grain map; recovery is one unfiltered pull per source; the gate opens
without the entries on an unknown previous owner, an exhausted pull, or a dead-but-listed-draining
previous owner. A miss on a live grain is a second activation
(`docs/design-notes-parity-gaps.md:219` says so), and nothing detects or repairs duplicates. The
protocol is about 520 lines inside `cluster-node.ts`, outside `@thresh/directory`. Orleans names an
explicit `RingRange` and `MembershipVersion` and has `CheckIntegrityAsync`.

**M7. Streams, reminders and durable jobs each answer "who owns this" differently, none fenced.** [read]
Streams and reminders use ring arcs over `activeSilos`; jobs use first-come store claims with
"dead" meaning "absent from my active set" (`durable-jobs/src/local-durable-job-manager.ts:226`).
Stream `seek` is an unconditional `SET`; reminder `recordFired` checks only the registration etag;
job persist hooks carry no owner. `silo-host.ts:264-266` starts partition work before readiness, so
every join overlaps. A readiness flap makes peers adopt work the silo is still running. Each job
adoption increments `adoptedCount` (`durable-jobs/src/redis-job-shard-store.ts:46`); past 3 the
shard is poisoned forever, `RELEASE` (`:57-61`) never resets it, and nothing logs it. Direction:
one partition-ownership abstraction with a stored epoch every write is conditional on.

**M8. Health signals do not measure what they name.** [read]
`hosting/src/health-check.ts:45-88`: `overloaded` is never set; `transportReady` is set once;
`membershipHealthy` is `silos.length > 0`, permanently true because of self-injection; `live()` is
unconditional; the watch exposes no connectivity state. A Kubernetes API outage freezes the view
with no signal.

**M9. Silo-to-silo replies use a second, reverse-dialled connection; responses match by id alone.** [read]
`cluster-node.ts:3535-3548,2154-2156`; `messaging/src/correlation-table.ts:67-75`. Fast-fail watches
only the request socket; a failed reply send is swallowed. `SiloAddress` has no process epoch and
correlation ids are a per-process counter from 1, so a late reply to a restarted container's
predecessor could complete an unrelated call [unverified].

## 2. Transactions

None of the departures below is recorded in `docs/deviations.md`.

**T1. The volatile caller-side agent drives 2PC; the TM is only a commit-record register.** [read]
`runtime/src/grain-factory.ts:475-483`; `runtime/src/transaction-agent.ts:96-145`;
`core/src/transaction-resource.ts:16-34`. The agent prepares every participant, calls
`recordCommit` on the elected TM, then commits every participant. Orleans' agent sends `Prepare`
and awaits `PrepareAndCommit` on the TM, whose `TransactionQueue` drives the rest. Thresh has no
`Prepared`, `Ping`, `Confirm`, `Cancel` or `CommitReadOnly`. T2–T4 follow from this.

**T2. `prepare` validates nothing.** [ran]
`transactions/src/transactional-state-impl.ts:223`:
`if (this.tentative?.transactionId !== transactionId) return true; // read-only participant`. The
access counter is shipped on replies and never checked. Probe: A debits 30, B credits 30, B's
activation is replaced before prepare; the transaction reports committed with A at 70 and B at 100.
Orleans: `ReaderWriterLock.ValidateLock` returns `BrokenLock` or `LockValidationFailed`.

**T3. `status(): boolean` conflates "not yet decided" with "aborted".** [ran]
`transactional-state-impl.ts:264-266,324-365`. A re-activated participant with a durable prepared
record asks the TM, gets `false` because no commit record exists yet, and drops the record. The TM
then records the commit; the participant's `commit` is a no-op. Probe: A committed, B's prepared
write gone. Orleans treats an in-queue transaction as undetermined.

**T4. Nothing in the protocol times out or is collected.**
- Lock holders never expire (`transactions/src/reader-writer-lock.ts:156-169` bounds waiters only).
  `transactions-options.ts:5-8` maps Orleans' 8 s `LockTimeout` (holder bound) onto the wait bound.
  Probe: an orphaned writer blocks every later transaction with wait-die indefinitely. [ran]
- A live prepared participant never asks for the outcome; `inDoubt` is filled only from `load()`. [read]
- Commit records are never removed; both providers rewrite the whole record per store. Probe: 500
  transactions leave 500 records. [ran]
- Protocol messages run as exclusive turns (`runtime/src/activation.ts:1187`); Orleans marks each
  `[AlwaysInterleave]`. This is the root of the tracked lock-wait-in-an-exclusive-turn follow-up. [read]

**T5. Root cause of #78: pending records are siblings, but the storage contract encodes a chain.** [read]
`transactional-state-impl.ts:206-208,345-354`; `core/src/transactional-storage.ts:66-81`.
`commitUpTo`/`abortAfter` are correct in Orleans because each transaction builds on the newest
pending state and aborts cascade. Thresh builds every tentative from the committed value and keeps
serving while a record is in doubt. Per-record operations are necessary but not sufficient: a live
transaction computed without an in-doubt predecessor's writes discards them if that predecessor
later commits.

**T6. Nothing blocks deactivation or migration while a lock or prepared record is live.** [read]
Orleans: `ActivationLifetime.BlockDeactivation()`.

## 3. Activation lifecycle

**L1. Deactivation has no single owner.** [read]
Seven sites compose the steps separately: `runtime/src/catalog.ts:617-643,314-331,722-756,344-357`;
`cluster-node.ts:3247-3258,2130-2133`; `activation.ts:506-530`. Idle migration runs
`onDeactivate` then dehydrates; the rebalancer, repartitioner and `TestCluster.migrate` dehydrate
then run the hook, with timers still armed across the hand-off. `deactivate()` is not joinable
(`activation.ts:818-836`): a second caller finalizes under the first caller's running hook. Invalid
entries are cleaned up by the next sweep or overwritten. Orleans: one `Deactivate` command, one
`FinishDeactivating` pipeline, one completion source.

**L2. Lifecycle operations are ordinary turns; a deactivation request is a polled flag.** [read]
`activation.ts:494,843,1054` schedule activate, deactivate and dehydrate as turns under request
interleaving rules, so on a reentrant grain `onDeactivate` and the dehydrate snapshot interleave
with running requests (`turn-scheduler.ts:313-319`). `deactivateOnIdle()` and an escaped
`InconsistentStateError` only set a flag (`activation.ts:992-1001,1151`); a grain under overlapping
traffic is never idle at lookup or sweep and keeps serving stale state. Orleans keeps a separate
operations queue and flips to `Deactivating` at once.

**L3. A 5-value state plus about 13 flags; admission is decided in about ten places that disagree.** [read]
`activation.ts:177,203-326`. Timer ticks check nothing (`:762`); `runStreamTurn` rejects with a
non-reroutable error (`:742-744`); `dehydrated` is honoured only by `invoke`.

**L4. Directory registration lives outside the activation; cold-grain order rests on microtask symmetry.** [read]
`distributed-dispatcher.ts:107-136,311-322`; `one-way-dispatch.ts:28-49`. The normal path registers
then creates; migration creates then registers. Per-caller order for a cold grain survives only
because certain lookups are deliberately not `async`. Root of the tracked exclusive-lock parity
test fragility. Orleans installs the activation synchronously and makes registration its first
lifecycle step.

**L5. The placement pipeline exists twice.** [read]
`cluster-node.ts:1936-1975` rebuilds placement for migration with a hand-made context: peers'
activation count is 0 and there is no `siloMetadata`, so `MetadataMatchFilter` prunes every
candidate and role-pinned grains can never migrate on idle. `isMigratableBy` checks only
`immovable`, so the rebalancer can pick a stateless worker.

**L6. The only environment-statistics source is a test stub, and its latches are public API.** [read]
`cluster-node.ts:566`; `runtime/src/load-shedding.ts:38-39`; `core/src/grain-runtime.ts:135-154`.
Gateway load shedding, overloaded-silo exclusion and resource-optimized scoring are inert in
production. Any application grain can call `runtime.latchOverloaded()` and take its silo out of
client traffic and load-aware placement.

**L7. Idle collection is a sequential full-map sweep that re-arms while a sweep is in flight.** [read]
`catalog.ts:677-678`; `activation-collector.ts:33-35`. Throughput is one deactivation per hook
latency.

**L8. A same-silo call has no default deadline; a cross-silo call has 30 s.** [read]
`hosting/src/silo-builder.ts:217-218`; `cluster-node.ts:684`. A co-located non-reentrant A→B→A
deadlocks until stuck detection (2 h default), contradicting `docs/guide/grains.md:71`.

## 4. Wire surface and location transparency

**W1. Dispatch by bare method name means there is no declared wire surface.** [ran]
`activation.ts:1293-1294` calls whatever function-valued property the name resolves to;
`runtime/src/grain-factory.ts:156-169` turns every property read on a reference into a remote call.
Observed, including through the external gateway client: `ref.onDeactivate()` ran a live
activation's deactivate hooks; `ref.setContext()` re-ran the factory and reset state;
`JSON.stringify({ grain: ref })` issued a remote `toJSON` call and left an unhandled rejection.
`JournaledGrain`'s `raiseEvent`, `clearLog` and `confirmEvents` are reachable the same way [read].
Orleans generates per-method invokables.

**W2. The value codec reserves keys inside user data.** [ran]
`core/src/value-codec.ts:34,450,717-726,740-742`. `{ $thresh: "date", value: 5 }` round-trips as a
`Date`; a nested `{ $thresh: "bigint", value: "abc" }` makes decode throw; `constructor`,
`prototype` and `__proto__` keys are dropped silently. The same codec backs every durable store.
BeneDB already defends itself by converting user structs to `Map`s.

**W3. Local calls bypass the codec, so placement decides call semantics.** [ran]
Arguments alias locally and copy remotely; `new Money(5)` arrives remotely as `{ cents: 5 }` with
no error; application error classes survive only locally; per-method options come from the caller
locally and the callee's registry remotely (`grain-factory.ts:169` vs `cluster-node.ts:3509`). No
switch forces wire semantics in tests, so a single-silo suite passes where multi-silo fails.

**W4. No compatibility contract for the wire or stored state.** [read]
One `CURRENT_VERSION` for every tag; a newer version is refused, an unknown tag degrades silently,
an unknown error falls back — three policies. `PROTOCOL_VERSION` is strict equality
(`web-socket-transport.ts:72-83`). Registries are process-global and filled by import side effect.
No schema version or upcast hook for application state or journal events. Stored layouts are
unversioned now that migrations are removed: an older Redis layout reads as empty.

**W5. A decode failure is a process fault.** [ran]
The listener deserializes unguarded and 9 of 15 system handlers have no failure path. One 3-byte
frame after a valid preamble raised `uncaughtException: DecodeError`. The preamble needs only the
cluster id, over `ws://`; `useWebSocketTransport()` has no TLS or auth options.

**W6. "Safe to resend" has three definitions.** [read]
Kind-based (`isStaleActivationRejection`), identity-based (`activation.isRerouteRejection`) and
"any failure including timeout" (`ClientNode`). One `try` in `receiveRequest`
(`cluster-node.ts:3432-3476`) covers routing and the turn, so a response cannot say "refused before
execution". Root of two tracked follow-ups. At least eight retry paths have separate bounds.

**W7. The error taxonomy lives in parallel hand-maintained tables that have drifted.** [ran]
`TransactionAlreadyResolvedError` is unregistered and arrives as a fallback class that is not a
`TransactionAbortedError`. `isThreshRuntimeError` is false for `InconsistentStateError` and the
transaction errors, which are `OrleansException`s upstream.

**W8. `GrainId` has two equalities.** [read]
`core/src/grain-id.ts:22,33,37`: `equals` includes the key kind; `toString`, the ring hash and the
catalog key drop it. Root of the deferred key-kind assertion.

## 5. Verification model

**V1. The test harness is a parallel wiring exactly where production differs.** [read]
`testing/src/test-cluster.ts:186-193,379-383`. One shared static membership gives every silo the
same view atomically; a kill is instant, cluster-wide and fail-fast, and the killed host keeps
running. 196 test files use the in-process transport, 3 use WebSocket. No test composes
`KubernetesMembership` with `SiloHost` and `ClusterNode`. M2–M5 are invisible to it.

**V2. The memory stream provider is a different model from the pulling providers.** [read]
It has per-subscription cursors, `startToken`, batches and filters; the pulling providers have
none. The shared provider has single-slot delivery bindings that each silo's `build()` overwrites
(`silo-builder.ts:1950`), so the last-built silo delivers every implicit subscription.

**V3. Three host layers with different defaults.** [read]
`Silo` plus `LocalDispatcher` has no production importer and 17 test files; reentrancy,
interleaving, timers and lifecycle are pinned there. The two dispatchers have already drifted.

**V4. The parity metric does not cover the largest deviation or the recommended authoring model.** [read]
Of 741 exclusions, 48 are Orleans' membership-protocol tests. `packages/parity` has 78 files using
`@grain(` and none using `defineGrain`; BeneDB uses classes only.

**V5. No fault-injection seam.** [read]
`InProcessNetwork.deliver` cannot drop, delay or partition. BeneDB built its own fault-injecting
storage, journal and schedule.

## 6. State and journaling

**S1. The journal adaptor swallows the "no longer the single writer" signal and stays stale.** [ran]
`journaling/src/log-view-adaptor-impl.ts:126-127` returns `false` on `InconsistentStateError`
without resyncing, so the deactivate-on-inconsistent-state net never fires. Probe: after one
foreign write, five conditional raises all returned `false` with the view at `{ count: 1 }` against
storage `{ count: 11 }`. BeneDB's `DatastoreGrain` treats `false` as a retryable race. The
custom-storage adaptor re-reads after a failed CAS; the two adaptors disagree.

**S2. One storage write per durable-collection mutation, with no atomic unit across structures.** [read]
`journaling/src/durable-dictionary-impl.ts:48-51`. Orleans buffers and flushes all structures in
one `WriteStateAsync`.

**S3. `JournaledGrain` event history is truncated by the shared compaction threshold.** [read]
`journaling/src/state-machine-manager-impl.ts:144`; `log-view-adaptor-impl.ts:235-243`. Orleans'
log storage keeps the whole log. Undocumented.

**S4. Log sharing is inferred by object identity over a constant log name.** [unverified]
`"journal"` is hard-coded in three binders; two named providers on the same tables look unshared.

**S5. No shared facet contract.** [read]
Built-in facets are enumerated by hand in `silo-builder.ts:1776-1818`, `activation.ts:1192-1235`
and `runtime/src/migration-participants.ts:13-20`. #94, #117 and the `@readOnly` breadth work were
missed enumerations. Migration behaviour differs per mechanism with no stated rule.

**S6. The in-memory grain store is not a model of the durable ones.** [ran]
It uses `structuredClone`; Redis and Postgres use the value codec. A `GrainId` in state returns as
a plain object from memory; a `RegExp` survives memory and throws on the codec.

**S7. The storage-backend seam covers three of six durable stores.** [read]
Transactional storage still defaults to per-silo memory; there is no Postgres transactional or
job-shard store; `transactional-state-activator.ts:32` ignores a non-default provider name.

**S8. `defineReducerGrain` effects are neither atomic nor durable.** [read]
`core/src/define-reducer-grain.ts:101-112` assigns state before the write with no rollback and
dispatches effects after it; a failed effect plus a retry folds the action twice.

## 7. Streams, reminders and durable jobs

**B1. Pulling-stream delivery state is one cursor per physical queue.** [read]
`streams/src/queue-pulling-agent.ts:179-185,219`; `pulling-stream-provider-core.ts:249,298-299`.
Shared root of #97's residual and #112. Also: head-of-line blocking across unrelated streams,
throughput independent of silo count, and a recoverable error rewinds the whole queue.

**B2. Retention and rewind are not part of the queue seam.** [read]
Redis never trims; Postgres deletes at or below the cursor on commit, so a rewind may find its rows
gone; Kafka relies on topic retention.

**B3. Durable-job shards are cluster-wide time buckets.** [read]
`durable-jobs/src/job-model.ts:11`. Every job due in one bucket runs on one silo. Scheduling on a
non-owner forwards best-effort; a running executor never reloads, so a lost forward strands the
job; 13 persist and release calls swallow errors; Redis job keys carry no `serviceId`. Orleans
shards per silo and has no forwarding.

**B4. A reminder registered on a non-owner loses its first tick.** [ran once]
`reminders/src/local-reminder-service.ts:99`. `due: 5s, period: 1h` first fired at one hour and
five seconds. There is no owner-routed system call; Orleans routes registration to the range owner.

**B5. #99 has its own root.** [read] The `PullableQueue` contract assumes token order equals
visibility order.

**B6. Kafka holds no authoritative assignment or offsets.** [read] Each process creates a
throwaway consumer group, so a Kafka deployment still needs Redis or Postgres for cursors.

## 8. Composition, configuration and operability

**C1. The builder accumulates side effects at call time; validation is a list of special cases.** [read]
`silo-builder.ts:529-544,1564-1645`. Resources are created when a method is called, `build()`
mutates the builder, ordering matters silently, and in-memory detection is `instanceof`, which a
wrapper evades. Still accepted on a cluster: Kubernetes membership with no health endpoints,
`maxRequestProcessingTime` at or below `callTimeout`, `gracefulShutdownMs` above `stopBudgetMs`.

**C2. Options live in four hand-synchronised layers.** [read]
`SiloConfig` (24 fields), 55 builder methods, `ClusterNodeOptions` (39), `TestClusterOptions` (22).
Eight `SiloConfig` fields cannot be set on a `TestCluster` silo; defaults differ by layer.

**C3. Lifecycle is hook arrays, not a staged lifecycle.** [read]
Stop is not the reverse of start; one throwing `onStop` hook skips the rest; a failed start leaves
listeners and timers running; `useGrainActivator` is a single slot.

**C4. The logger is not a seam and the runtime is silent by default.** [read]
`useLogging` also installs a per-call filter; about 20 log sites in the runtime, one in
`cluster-node.ts`. BeneDB's hosts and every example run with all diagnostics discarded. No metrics
for membership, the scheduler or transactions.

**C5. `ClusterNode` is a god object standing in for a missing system-target abstraction.** [read]
3,549 lines, 59 fields, about 143 methods. Control traffic is a 15-value enum and a 15-arm `if`
chain; the request envelope is built by hand in three places and the error codec is duplicated in
the client.

**C6. `GrainRuntime` is a god-interface** (about 29 members), so core owns every subsystem's contract. [read]

**C7. The silo builder statically imports `redis`, `pg` and `kafkajs`**, so every host loads every driver. [read]

## 9. API boundary and consumer coupling

**A1. Nothing separates public API from internals.** [read]
Every package exports `./*` from `src`. BeneDB's production code imports 38 distinct Thresh
modules, including `runtime/construct-grain`, `core/batch-worker` and `testing/test-cluster`.
`CLAUDE.md`'s public-API rule has no boundary to apply to.

**A2. Nothing runs BeneDB when Thresh changes.** [read]
BeneDB's CI checks out Thresh `main` on BeneDB pushes only, on a self-hosted runner.

**A3. Two authoring models are not in lockstep.** [read]
Class-only: per-silo constructor dependencies, `@mayInterleave`, `@grainFacet`, regex channel
subscriptions, `JournaledGrain`, custom storage. Functional-only: composable lifecycle hooks,
`defineReducerGrain`.

## What is sound

- No dependency cycles in production source; `core` is a leaf; the clock is injected almost everywhere.
- `TurnScheduler` is a tight port of `MayInvokeRequest`; the admission rules themselves are right.
- The storage providers are careful about CAS atomicity, and `CustomStorageLogViewAdaptorImpl` is a
  clear, well-argued port.
- The Kubernetes watch itself (list, then watch from `resourceVersion`, jittered backoff) is sound.
- The error codec's trust model is sound: closed tables, no name-to-class resolution from the wire.
- Rules of hooks hold by structure, not convention.
- Drain ordering is carefully reasoned and mirrors Orleans.
- Comments record why each decision was made, which is what made the gaps findable.

## Probes

The reproductions behind every **[ran]** and **[ran once]** finding were throwaway scripts kept
outside the tree. They are not committed: each fix ports its probe into the suite as a test that
was observed failing first, so the suite is the lasting record. `todo.md` indexes the findings and
their fix status.
