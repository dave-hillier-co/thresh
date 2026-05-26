# ADR 0008 — Cross-grain ACID transactions

- Status: Accepted (design; implementation in progress — Phase 7)
- Context docs: [07 — Persistence](../07-persistence.md),
  [02 — Actor model](../02-actor-model.md), [13 — Roadmap](../13-roadmap-and-phases.md),
  [ADR 0005 — Redis defaults](0005-redis-default-providers.md)

> Orleans references (the protocol this ADR ports, faithfully):
> `Orleans.Transactions/DistributedTM/TransactionAgent.cs` (the agent: start, collate participants,
> elect the TM, drive prepare/commit/abort), `.../ParticipantId.cs` (Resource/Manager/PriorityManager
> roles), `.../TransactionManagerExtension.cs`;
> `Orleans.Transactions/State/{TransactionalState,TransactionQueue,ReaderWriterLock,TransactionManager}.cs`
> (the resource, its commit queue, and the wait-die lock); `Orleans.Transactions/Utilities/CausalClock.cs`
> (monotonic logical timestamps); `Orleans.Transactions/Abstractions/{ITransactionManager,ITransactionalState,ITransactionalStateStorage}.cs`
> (the `PrepareAndCommit`/`Prepared`/`Ping` TM contract, the `performRead`/`performUpdate` facet, and
> the durable load/store contract).

## Context

Orleans parity ([01](../01-overview-and-goals.md)) requires **distributed ACID transactions across
grains** — Orleans Transactions: a single logical transaction spans any number of grains in a call
graph, commits atomically, and is serializable and durable. Today persistence is per-grain etag
optimistic concurrency ([07](../07-persistence.md)): a single grain's `write()` is atomic, but there
is no way to make a change across two grains all-or-nothing. A bank transfer (debit grain A, credit
grain B) can half-apply if the second call fails. This ADR adds the transaction machinery; it is
additive — grains that don't opt in are unaffected.

## Decision

Mirror Orleans' **actual** distributed transaction protocol — not a generic central-coordinator 2PC
and not pure version-validation OCC, but Orleans' timestamp-ordered, lock-based optimistic protocol
with the transaction manager co-located on a participant. Declarative transaction boundaries on
methods, a transactional-state facet grains read/write inside a transaction, and an optimistic,
serializable commit with a TM elected from the writers and durable recovery.

1. **Transaction boundaries.** A method option `transaction: TransactionOption` (mirroring Orleans:
   `Create`, `CreateOrJoin`, `Join`, `Supported`, `NotAllowed`, `Suppress`) declares how a call
   relates to the ambient transaction. `Create`/`CreateOrJoin` start one if absent; `Join` requires
   one. The transaction **agent** (TA) begins a transaction by assigning a globally unique id and a
   **logical timestamp** from a per-silo `CausalClock` (monotonic — `max(previous+1, wallclock)` —
   so timestamps strictly increase and merge across silos, giving every transaction a total order
   that doubles as its **priority**). The id, timestamp, and a per-participant **access counter**
   (reads/writes) propagate through the **request context** — the same channel that already carries
   the reentrancy/chain id ([04](../04-messaging-and-serialization.md)) — so a transaction flows
   across silos with the call.

2. **Transactional state facet.** A `TransactionalState<T>` facet (alongside `@persistentState`)
   holds versioned state and is accessed only as `performUpdate(fn)` / `performRead(fn)` within a
   transaction. Each facet is a **resource** that participates in the transaction: it keeps a stable
   committed version (with a dense local sequence number) plus per-transaction **tentative** writes,
   tracks the reads/writes each transaction performed, and **enlists** itself with the TA on first
   access. Outside a transaction, reads see the committed version.

3. **Locking with wait-die (serializable isolation).** A resource serializes access through a
   **reader-writer lock granted in timestamp order**. Concurrent reads don't conflict; a write
   conflicts with any other access. Deadlock is prevented by **wait-die** keyed on the transaction
   timestamp/priority: when a transaction requests a lock that conflicts with a holder, the **older**
   transaction (smaller timestamp) waits while the **younger** (larger timestamp) aborts ("dies") and
   the caller may retry. Because the wound direction is fixed by a total timestamp order, no wait
   cycle can form. This is how isolation is enforced — not by post-hoc version validation alone.

4. **Commit: TM elected from the writers, prepare + commit.** On commit the TA **collates** the
   enlisted participants and **elects the transaction manager (TM)** from them — a designated priority
   manager if present, otherwise the first resource that performed a write (`ParticipantId` carries
   `Resource` / `Manager` roles). The TM role is thus co-located with the transaction, with no central
   bottleneck. A **read-only** transaction skips the TM: the TA calls `commitReadOnly` on every
   resource (validating the versions read are still current) and aborts all on any failure. A
   **read-write** transaction runs two-phase: the TA sends a **one-way `prepare`** to every resource
   except the TM (each validates its lock/versions, durably stages its tentative state and a prepare
   record, then reports `prepared`), and awaits **`prepareAndCommit`** on the TM, which prepares
   itself, confirms all participants prepared, durably logs the commit, and returns the outcome. On
   any prepare failure, lock-validation failure, or timeout the transaction **aborts**: the TA (or TM)
   sends one-way `cancel`/`abort` to the participants, which discard tentative state and release locks.

5. **Durability and recovery.** Committed transactional state and the commit records are durable
   (Redis by default, via a versioned storage contract — committed state + committed sequence id +
   pending prepared states + a commit-records metadata map). A silo failure mid-commit leaves in-doubt
   participants holding prepared-but-uncommitted records; on restart each participant resolves its fate
   from the recorded TM `ParticipantId` (asks the TM or its successor), so the outcome is consistent —
   committed or aborted, never torn.

## Consequences

- A new storage contract distinct from `PersistentState`: versioned state with a sequence of pending
  tentative writes and a commit-records log, not a single etag record. The two facets coexist on a
  grain.
- Commit adds round trips (one-way prepares + the TM's prepare-and-commit) and cross-silo hops for
  remote participants — the cost of distributed ACID; only transactional calls pay it.
- At-least-once commit application must be idempotent at each participant (recovery may re-apply from
  the prepared record).
- Serializable isolation via wait-die means contended transactions abort and retry; the younger
  transaction is the one that dies, so progress is guaranteed (the oldest never aborts on a lock
  conflict). Callers handle retry, as in Orleans.

## Alternatives considered

- **Pure version-validation OCC (validate-at-prepare only, no locks).** Simpler — no lock manager or
  wait-die — but it is *not* how Orleans achieves isolation, so it is a parity gap; it also degrades to
  high abort rates under contention without the timestamp-ordered wound direction. Rejected for
  fidelity.
- **Central-coordinator 2PC.** A standalone TM is a bottleneck and a single point of failure; Orleans
  deliberately co-locates the TM on a participant. Rejected.
- **Deterministic transactions (Snapper-style).** Higher throughput under contention, but requires
  pre-declared access sets and a batching layer — a larger departure from the Orleans programming
  model we are matching. Could layer on later behind the same facet.
- **Sagas / compensation.** Eventual, not ACID; pushes correctness onto application code. Not parity.
- **Single-grain only (status quo).** Insufficient — the defining feature is multi-grain atomicity.

## Implementation slices

1. **Transaction context + boundaries.** `TransactionOption` on method options; the TA assigns id +
   `CausalClock` timestamp; the proxy/dispatcher begins/joins a transaction and propagates the context
   (id, timestamp, access counters) through the request context across silos. An in-memory TA/TM,
   single silo, no durability. Failing test first: two grains updated in one transaction, an induced
   failure aborts both (no half-apply).
2. **Transactional state facet.** `TransactionalState<T>` with versioned state, per-transaction
   tentative writes, read/write access tracking, and the **wait-die reader-writer lock**;
   `performUpdate` / `performRead`. Sociable tests over the in-memory path (tentative writes invisible
   outside the tx; two contending transactions ordered deterministically — the younger dies).
3. **Optimistic serializable commit.** TM elected from the write participants; one-way prepare +
   `prepareAndCommit` + prepared/cancel across participants (routed as system extensions over the
   dispatcher); cascading abort. End-to-end: a transfer across two account grains is atomic,
   concurrent transfers serialize, and a conflicting transaction aborts.
4. **Durability + recovery.** Persist committed transactional state and commit records (Redis);
   resolve in-doubt transactions on restart from the recorded TM; remote participants over the
   dispatcher. Multi-silo end-to-end: a silo dies mid-commit and the outcome stays consistent.
