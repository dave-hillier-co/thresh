# Project review — 2026-09-22

Second review, following [`project-review-2026-08-04.md`](project-review-2026-08-04.md). That review
worked from documentation and a stub search, and its test run was interrupted; this one ran the
suites, the examples and the scorecard, and read the correctness-critical paths directly. The
packaging/release question is out of scope by request and is not covered here.

## Scope and evidence

Commands run, all on a clean `main` at `38bbb5a`:

| Check | Result |
| --- | --- |
| `pnpm typecheck` | clean |
| `pnpm lint` | clean |
| `pnpm test:all` | 1903 passed, 0 failed, 274 skipped, 435 files, 58s |
| `pnpm test:parity` | 709 passed, 0 failed, 98 skipped, 67s |
| `pnpm parity:scorecard` | 502 ported / 0 gap / 475 excluded |
| `examples/cluster` | runs; cross-silo routing and failover both demonstrated |
| `examples/{greeter,chat,bank,broadcast}` | run correctly |
| `examples/migration` | **prints correct output, then exits non-zero** (F16) |

Also read: `packages/directory/src/**`, the directory and recovery paths of `cluster-node.ts`,
`turn-scheduler.ts`, `activation.ts`, `distributed-dispatcher.ts`, `packages/transactions/**`,
`transaction-agent.ts`, `packages/parity` tooling, and the rule that no `index.ts` barrel exists
anywhere outside `node_modules` (it holds).

## Verdict

The runtime is real and does what it claims structurally, and the code hygiene is better than most
production code: zero `TODO`/`FIXME`/`HACK` in production source, 16 type escapes across ~95k lines,
and **zero `vi.mock` with 12 mock/spy usages across 221 test files and 4209 assertions** — the
suite is genuinely classic/sociable, as `CLAUDE.md` asks. The turn scheduler is a faithful
transcription of `ActivationData.MayInvokeRequest`, and
[`simplification-opportunities.md`](simplification-opportunities.md) records 44 applied
simplifications *and* the rejected candidates with reasoning.

Two things do not hold up. First, the **directory's failure paths** contain several defects that
land squarely on Kubernetes rolling updates (F1–F7), including duplicate activations with divergent
state. Second, the **parity metric does not measure what its headline implies** (F17–F20): the
scorecard cannot see 267 of its own exclusions, "0 gap" was reached partly by reclassifying 67
gap-tagged tests as excluded, and the suite never runs in CI.

Findings are numbered F1…F22 for reference from `todo.md`. **F1–F18 are filed as issues
[#67–#84](https://github.com/dave-hillier-co/thresh/issues?q=is%3Aissue+67..84)**; F19–F22 and the
documentation corrections remain inline here and are indexed in `todo.md`. Severity is my own
judgement:
**[high]** = wrong behaviour reachable on a normal production path; **[med]** = reachable but needs
an interleaving or a misconfiguration; **[low]** = correctness-adjacent or documentation.

---

## Directory

### F1 — A readiness flip during graceful drain deletes entries for a silo that is still serving **[high]**

`graceful-shutdown.ts:42-48` flips readiness, then waits `graceMs` (default 5000) before stopping
anything. Meanwhile peers classify by readiness: `updateView` drains its partition with
`if (!live.has(entry.silo.ringKey)) return "drop"` (`cluster-node.ts:1147-1150`, commented "host
gone — grain reactivates"), and `live` comes from `readySilosFromSlices`, which excludes any endpoint
whose ready condition is false (`endpoint-slice.ts:47-49`).

So for the whole drain window every peer permanently deletes every directory entry pointing at the
draining silo, while those grains are still live and serving turns. A call for one of them finds no
entry and builds a second activation: a rolling update produces duplicate activations cluster-wide.
The same trigger fires on a transient readiness blip, including the self-probe's `dispatcherResponsive`
check — the silo is alive by definition in exactly the case that flips it.

Orleans removes entries for a silo only once its liveness vote expires, not on a single readiness
observation. Fix direction: distinguish "draining" from "gone" when classifying entries.

### F2 — A silo that regains a range never pulls it, so the entry is stranded and expires **[high]**

`beginRecovery` runs only on JOIN (`start()` at `cluster-node.ts:1106`; `updateView`'s `!wasActive`
gate at `:1165`). A silo that gains a range because another silo *left* never pulls, so the previous
owner's `handoffSnapshot` entries sit unpulled until `recoveryRetentionMs` (60s) expires.

Reproduced by execution: the range returns to an already-active silo and is not recovered **even when
that silo holds the entry in its own snapshot**. A call then rebuilds the grain — two live
activations, divergent state (2 vs 6) — and on expiry the only surviving copy is deleted. Already
recorded in `todo.md` as "an incumbent that gains a range never pulls it"; this sharpens it into a
duplicate-activation invariant violation. Orleans runs `AcquireRangeAsync` per partition per view
change.

### F3 — The recovery ACK is identity-blind, so a late ACK deletes a newer entry **[med]**

`ackServedRecovery` deletes by `grainId.toString()` (`cluster-node.ts:1308-1312`) while the code
already holds the full `GrainAddress`. A delayed ACK from an earlier handoff deletes a newer entry
registered under the same grain id in the meantime, and the newer entry exists nowhere else. Deleting
only on an exact `GrainAddress` match (or carrying a per-entry `producedAt`) closes it.

### F4 — `updateView()` throwing kills the membership watch loop permanently **[med]**

`silo-host.ts:263-278` drives the watch with no try/catch. One throw rejects the `for await`, the
loop exits for good, and the silo serves with a frozen ring, silently diverging.

**Correction (2026-09-22, from the implementation pass).** The trigger originally given here — an
empty ring reaching `ownerOf` (`consistent-hash-ring.ts:57`) via `drain`'s classify — is **not
reachable**: `live` (`cluster-node.ts:1132`) and `newRing` (`buildRing`, `:1170`) are built from the
same `activeSilos(snapshot)`, so an empty ring means an empty `live` set and `drain` returns `"drop"`
before `ownerOf` is called. The real throwers are the loop body's other work: the `onOwnershipChange`
hooks do store I/O (durable-job shard claims, pulling-stream queue ownership) against
Postgres/Redis/Kafka, so one store blip freezes the view permanently. `route` on an empty ring does
still throw a plain `Error` rather than a `RejectionError`, and `ClusterNode.applyDirectoryOp`'s
`ownsNow` retains the same empty-ring hazard.

### F5 — A directory op can register into a partition that no longer owns the range **[med]**

Both remote and local paths decide owner/version, `await` something, and only then touch the
partition: the remote path checks the version at `cluster-node.ts:2649` and then `await`s
`awaitRecovered` before acting; the local path picks `owner` at `distributed-grain-directory.ts:89`
and then `await`s `onOwnedAccess` before `onOwned()`. `awaitView` is careful here (it advances and
re-checks); `awaitRecovered` is not. A view change landing in that yield writes into a partition whose
`drain` has already run, and nothing re-drains it until the next view change.

`local-directory-partition.ts:10-18` asserts the barrier makes this impossible; it makes it narrow,
not impossible.

### F6 — Production membership versions are per-silo counters, so `staleView` cannot mean what it claims **[med]**

`KubernetesMembership` numbers views with its own counter (`kubernetes-membership.ts:83`,
`version: this.snapshot.version + 1` per reconciliation), while tests and dev clusters share one
`StaticMembershipService` so version N means the same view everywhere
(`test-cluster.ts:146-149` documents that invariant as if it held generally). In production identical
numbers denote unrelated views. Two consequences: a false `staleView` (bounded retries, then a hard
error to the application), and the reverse — a genuinely divergent ring compares "equal", so two
partitions can each hold a valid entry for one grain.

Relatedly, `KubernetesMembership.onSlices` always injects `this.local`, so a removed or draining silo
keeps itself in its own ring indefinitely while every peer has dropped it — a guaranteed divergence
the version check cannot see.

### F7 — Recovery is one-shot, silently swallowed, and gates all owned directory traffic **[med]**

`cluster-node.ts:1249-1298`: the `catch {}` at `:1289-1291` swallows a fully exhausted recovery with
no log, metric or retry re-arm, and the budget is 3 attempts × 200ms with no jitter — one shot for
the process lifetime, ~400ms of tolerance. `awaitRecovered` then awaits the *whole* multi-source
promise and gates every lookup/register/unregister for locally-owned grains, each attempt bounded by
`callTimeoutMs` (30s default), so one slow-but-in-view peer can block this silo's directory traffic
for up to ~90s. The design notes want a "lookup resolved via lazy activation" counter; there is none.

---

## Placement and routing

### F8 — Cross-silo placement load is degenerate: peers report `activationCount` 0 **[high]**

`placementContext` returns `activationCount: (silo) => (isLocal(silo) ? this.catalog.count() : 0)`
and `resourceStats: … : undefined` (`cluster-node.ts:1409-1416`), with the comment "there is no
cross-silo load gossip yet". Every remote silo therefore scores **zero activations**, so
`ActivationCountPlacement` (`activation-count-placement.ts:26-35`) always prefers a remote silo over a
loaded local one — the signal is not merely uninformative, it is inverted. `ResourceOptimizedPlacement`
(`resource-optimized-placement.ts:21-31`) scores by activation count alone and never consults its
Orleans weights; the option surface in `resource-optimized-placement-options.ts` is imported only by
its parity test.

The real peer counts are already in memory: `remoteLoadStats` is populated at `cluster-node.ts:2126`
and read only for `isOverloaded` (`:1417`). This is a wiring fix, not a design one.

### F9 — Stateless-worker calls that arrive over the wire bypass the worker pool **[high]**

`placeAndInvoke` short-circuits to `catalog.pickOrScaleWorker` for stateless types
(`distributed-dispatcher.ts:166-168`), but inbound wire messages go to `deliverLocal`
(`cluster-node.ts:2745`), which has no stateless check and falls through to directory registration
plus a single-activation `catalog.getOrActivate`. So a client's calls to a stateless-worker grain get
pool-of-one semantics, while concurrent grain-to-grain calls to the same id get a scaled local pool.

The comment at `distributed-dispatcher.ts:158-167` asserts "a stateless-worker call always resolves on
whichever silo makes it, exactly like Orleans" — true for silo-originated calls, false for every call
that arrives over a connection. Nothing in `packages/client` or `packages/messaging` excludes stateless
types (zero `stateless` references), and `StatelessWorkerPlacement.choose` is unreachable on any
production path.

---

## Transactions

### F10 — `ReaderWriterLock.release()` orphans a queued waiter, hanging the turn forever **[high]**

`reader-writer-lock.ts:111-121` removes a waiter's entry for the released transaction
(`waiters.splice(i, 1)`, `clearTimer`) but never settles its promise, and `pump()` cannot help because
the entry is gone. Reproduced deterministically on `ReaderWriterLock` directly: the awaiting
`performUpdate` never settles, so the grain's exclusive turn never completes and the activation stops
serving calls entirely — not just for that transaction.

Reachable whenever a transaction holds a read lock and is queued for an upgrade on the same resource,
then is aborted elsewhere (sibling participant dies, caller deadline fires, root `Promise.all`
rejects). The deadline path at `:148-155` already settles its promise; `release` does not.

### F11 — A participant that enlists after `resolve`/`abort` keeps its lock forever **[high]**

`transaction-agent.ts:64` and `:121` snapshot `[...info.participants.values()]` once. A resource
enlisting afterwards is never prepared, never committed, and never aborted, so its lock is never
released — the only release points are `commit`/`abort`. Two verified triggers, both normal:

- **`oneWay` + `transaction: "supported"`.** `grain-factory.ts:355-368` rejects
  `create`/`createOrJoin`/`join` for one-way calls but not `supported`, whose comment claims
  `suppress`/`supported` "never make this call a transaction boundary" — true, but `supported` still
  *joins* the ambient transaction, so a detached callee's writes attach to a transaction the root
  resolves before the callee's turn runs. Reproduced: after the root commits, two consecutive reads
  of that resource fail with `wait-die: younger than a lock holder`, and the callee's write is
  silently discarded.
- **`Promise.all` with one dying branch.** Reproduced: `doomed` dies under wait-die, the root aborts,
  and the still-sleeping `slow` branch later takes a write lock nothing releases.

Either way the resource is unusable until the activation deactivates, at which point the write is
lost. Root cause is shared: mark the transaction resolved so a late enlist releases immediately or
throws.

### F12 — In-doubt resolution can promote a live transaction's tentative state **[high]**

`transactional-state-impl.ts:165` sets `sequenceId: this.committedSequenceId + 1`, ignoring
unresolved pending records, and `transactional-storage-apply.ts:47-49` replaces a pending state by
`sequenceId`. A resource that stays usable while a record is in doubt therefore hands the same
sequence id to the next transaction, clobbering the in-doubt record; `resolveOne` (`:266-295`) then
commits with `commitUpTo = pending.sequenceId`, promoting whatever pending record now sits there.

Reproduced end to end: T1 in doubt → T2 writes and prepares → recovery answers "T1 committed" →
promotes seq 1 → **the durable value becomes T2's never-committed tentative**. T2 then aborts and the
caller is told `aborted`, while T2's write stays durably committed. A transaction whose reported
outcome is abort is nevertheless durable.

### F13 — `recordCommit` mutates in memory before the durable write, so `status` can disagree **[high]**

`transactional-state-impl.ts:198-209` assigns `this.metadata` and only then awaits `storeState([])`;
`status` (`:211-214`) reads `this.metadata`. After a failing `recordCommit`, the live manager answers
`status("T1") === true` while a fresh activation over the same storage answers `false`. The agent
converts the failure to `TransactionInDoubtError` and leaves participants prepared, so a participant's
recovery query against the still-live TM gets "committed" and commits, while the TM's own pending
record will later resolve to abort. One transaction, two outcomes, decided by which activation
answers. The atomic commit point the protocol depends on is not what `status` reports.

### F14 — TM election can name a participant that has no durable commit point **[high]**

`electManager` returns the first write participant in Map-insertion order
(`transaction-agent.ts:186-189`), and `TransactionCommitter.recordCommit` is a deliberate no-op
(`transaction-committer.ts:92-95`, "never the elected manager in the ported test scenarios"), while
`TransactionalStateImpl.recordCommit` is the real one. A transaction whose first writer is a
committer therefore has no durable commit point at all, and the `TransactionParticipant` contract
omits `status` (`core/src/transaction-info.ts:58-72`) while `TransactionResource` requires it — so
recovery of a sibling routes `status` to that grain, `invokeTransactionResource` finds no such
transactional field and throws, and the record stays in doubt while the confirmation worker retries
forever.

Not hypothetical: `toc-fault-transaction-memory-tests.test.ts` does
`Promise.all([...grains.add(...), committer.commit(...)])`, so which writer lands first — and hence
whether the committer is TM — depends on reply order.

### F15 — The confirmation worker dies silently on a store error **[med]**

`transactional-state-impl.ts:313-320` awaits `resolveOne` with no guard, and its `storeState` can
throw (etag conflict with the live activation, provider error). The throw escapes a
`void this.runConfirmationPass()` call: unhandled rejection, worker never re-armed, and the in-doubt
record is never retried for the life of the activation.

---

## Runtime

### F16 — A dangling call's timeout rejection is unhandled and can kill the process **[high]**

`examples/migration` prints all of its correct output and *then* crashes 30s later:

```
GrainCallTimeoutError: grain call 3 timed out after 30000ms
    at Timeout._onTimeout (packages/messaging/src/correlation-table.ts:41:18)
```

Reproduced with a probe that omits `process.exit`: after `cluster.stop()` resolves, the pending
call's timer fires and its rejection reaches no handler. Under Node's default
`--unhandled-rejections=throw` that terminates the process with a non-zero exit code.

This is more than a broken example. A call abandoned during shutdown (or any caller that stops
awaiting) leaves a correlation entry armed; when it times out, nothing catches the rejection. The
example is the visible symptom; the class is "an abandoned grain call can kill a silo process".

---

## Serialization

### F17 — `$tsvv` is written but never read **[low]**

`value-codec.ts:314` stamps `[V]: CURRENT_VERSION` on every tagged value, and nothing consumes it:
the decoder treats `$tsvv` only as a key to skip, and no other file in `packages/` references it. The
comment at `value-codec.ts:38` refers to a `versionOf` function "below" that does not exist. A payload
stamped `$tsvv: 2` decodes as a plain object (`value-codec.test.ts:186`). Either implement the
reader or stop writing the field — as it stands EPICS lists a schema version that is inert.

---

## Parity suite and test infrastructure

### F18 — The parity scorecard cannot see 742 exclusions, and reports 475 **[high]**

Counted directly: **742 `orleansTest.excluded(` call sites and 545 `orleansTest(`**. The scorecard
prints 475 and 502. Its regex (`scripts/parity-scorecard.ts:35-36`) requires an *inline string
literal* as the first argument, so the 267 exclusions written as
`orleansTest.excluded(REASON, …)` are invisible — as are 43 ports whose ids are template literals.
24% of declarations are outside the scorecard's view, and the numbers are load-bearing in
`EPICS.md`, `todo.md`, and the 2026-08-04 review.

### F19 — "0 gap" was reached partly by reclassifying gaps as exclusions **[high]**

At the port commit `c7a5113` there were 488 `gap` call sites; today there are zero, and `excluded`
grew from 646 to 742. 283 tests genuinely became ported — real progress. But 67 were moved from
`gap` to `excluded`, 57 of them the entire generic-grains family, and the gap section now prints
empty.

The generic-grains justification ("open generic grain interfaces are unrepresentable in this
framework") is defensible as a design boundary but overstates the case: TypeScript can express closed
instantiations; what it cannot do is reflectively instantiate from a wire-supplied type argument. The
distinction between *impossible here* and *not built here* is exactly what the taxonomy exists to
draw, and it is currently the difference between a 0-gap headline and an honest one. Note also that
11 of the 13 `GAP-*` tags referenced inside current exclusion reasons no longer exist in the `GapTag`
union, and `runtime/grain-directory-tests.test.ts`'s header still cites a stale reason.

Relatedly, `orleansTest.excluded` is a no-op (`orleans-test.ts:62`) — those tests do not even appear
as skipped — and `--run` never asserts `pass + fail === ported`, so a ported test that stops
executing degrades the pass column silently while the scorecard still exits 0.

### F20 — The parity suite never runs in CI **[high]**

`pnpm test` is `vitest run --project unit`, and the `unit` project excludes `packages/parity/**`
(`vitest.config.ts`). `.github/workflows/ci.yml` runs only `pnpm typecheck`, `pnpm lint`, `pnpm test`.
The 502-test suite that *is* the parity claim takes 67 seconds and is never regression-guarded.

### F21 — One parity file asserts against a shim and counts as 5 ported tests **[med]**

`runtime/grain-directory-tests.test.ts` reimplements the CAS contract in a 30-line `Map` wrapper and
registers five upstream ids against it. Those five pass even if `@thresh/directory` were deleted. The
file's stated justification — that the directory "is not a declared dependency of the parity
package" — is now stale: `packages/parity/package.json:19` declares it, and
`grain-directory/distributed-grain-directory-tests.test.ts` (written later) says so explicitly while
closing the same facts against the real implementation.

### F22 — Redis- and Kafka-backed tests vanish when the service is absent **[med]**

~176 unit tests skip silently without Redis and Kafka (this review's local run had Redis down and
Kafka down; Postgres was up and did run). CI provides Redis and Postgres but not Kafka, which its own
comment acknowledges. The skip mechanism is honest per file, but a green local run says less about the
storage surface than the file count suggests, and nothing reports the skipped total prominently.

---

## Documentation accuracy

The docs are unusually honest: roughly 80% of substantive claims checked out, including every symbol
named in the burn-down list. The divergences worth correcting, each verified against code:

| Doc | Claim | Reality |
| --- | --- | --- |
| `deviations.md:147-148` | `InconsistentStateError` carries "the expected and stored versions in the etag fields" | the stored version is deliberately unavailable (`custom-storage-log-view-adaptor-impl.ts:214-222`) |
| `deviations.md:31-37` | a top-level `undefined` is tagged, "only the top level" | positional slots are tagged too (`value-codec.ts:370-377`); `orleans-to-thresh-port.md:349` says so correctly, so the two docs disagree |
| `deviations.md:50-53` | gateways come from "Kubernetes `Service` / DNS" | no such provider exists; only static / membership / url (`gateway-provider.ts`), and nothing in `clustering-k8s` serves a gateway list |
| `deviations.md:22-23` | `GrainWith*Key` markers "are deprecated" | `EPICS.md:107-114` says the opposite and the code agrees with EPICS (`key-kinds.ts:32` is a plain alias) |
| `deviations.md:50` | membership is "a watch on Pod endpoints" | it is an EndpointSlice watch (`subscription: EndpointSlice`); the Pod watch is opt-in label enrichment |
| `orleans-to-thresh-port.md:359` | a RequestContext `set` inside an awaited callee "does leak back UP to the caller" | each incoming turn runs on a shallow copy (`activation.ts:358-369`), so it does not |
| `orleans-to-thresh-port.md:63` | `usePersistentState<T>(ctx, "name", …)` | the current signature takes no `ctx` (`define-grain.ts:485-500`) |
| `orleans-to-thresh-port.md:382` | placement candidates are sorted with `SiloAddress.compare` | no production code sorts placement candidates; the only use is a vnode hash tie-break |
| `orleans-to-thresh-port.md:380` | the dispatcher passes the whole `GrainId` to "every filter and strategy" | false on the migration path (`cluster-node.ts:1466-1470`) |
| `EPICS.md:25-27` | "optimistic two-phase commit" | pessimistic: locks are taken at first access and held to commit, and `prepare` does no conflict check (`transactional-state-impl.ts:152-190`) |
| `EPICS.md:84-85` | `@readOnly` guard "breadth remainder in `todo.md`" | stale; full-facet coverage shipped (`activation.ts:774-786`, `todo.md:10`) |
| `EPICS.md:11-12` | "versioned lossless range handoff" | the version is the membership view version, and past `recoveryRetentionMs` the entry is dropped |
| `EPICS.md:28-33` | "Durable journaling (`DurableGrain`)" | no `DurableGrain` class exists; journaling is facets/hooks on an ordinary grain |
| `README.md:144-156` | examples list; each runs "over in-memory providers and the in-process transport" | `broadcast` and `migration` are omitted from the list; `cluster` uses WebSocket and `k8s-silo` a real cluster |
| `README.md:140` | "`pnpm test` # run the Vitest suites" | it runs the `unit` project only; the parity suite needs `pnpm test:parity` |
| `todo.md:31-33` | #64 covered "every other durable store" | `RedisJobShardStore` keys are prefix-only with no `serviceId` (`redis-job-shard-store.ts:171-179`) |

Narrower-than-worded (real code, softer claim than the wording suggests): `MigrateOnIdle` lives on
`GrainRuntime`, not the management extension; the version-placement filter is best-effort with no
receipt-side validation; call filters are silo-global, not grain-type-keyed ("per-grain" means per
instance); `onDeactivate(reason, signal?)`'s signal has no supplier anywhere; the memory stream
provider is not behind the shared pulling-provider core.

## Delta against the 2026-08-04 review

That review concluded the stated aims were met with only beyond-parity and deferred work open. Its
positive findings on layout, stub absence and example integrity still hold. What it could not see is
everything above: it never ran the suites, took the scorecard's 502/0/475 at face value (and repeated
them as evidence), and treated the 475 exclusions as settled scope boundaries. F18–F20 are the
consequence — the metric it relied on is not the metric it appears to be.
