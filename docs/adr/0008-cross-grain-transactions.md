# ADR 0008 — Cross-grain ACID transactions

- Status: Proposed (design; implementation pending — Phase 7)
- Context docs: [07 — Persistence](../07-persistence.md),
  [02 — Actor model](../02-actor-model.md), [13 — Roadmap](../13-roadmap-and-phases.md),
  [ADR 0005 — Redis defaults](0005-redis-default-providers.md)

## Context

Orleans parity ([01](../01-overview-and-goals.md)) requires **distributed ACID transactions across
grains** — Orleans Transactions: a single logical transaction spans any number of grains in a call
graph, commits atomically, and is serializable and durable. Today persistence is per-grain etag
optimistic concurrency ([07](../07-persistence.md)): a single grain's `write()` is atomic, but there
is no way to make a change across two grains all-or-nothing. A bank transfer (debit grain A, credit
grain B) can half-apply if the second call fails. This ADR adds the transaction machinery; it is
additive — grains that don't opt in are unaffected.

## Decision

Mirror the Orleans model: declarative transaction boundaries on methods, a transactional-state facet
that grains read/write inside a transaction, and an optimistic, serializable commit protocol with a
transaction manager and durable recovery.

1. **Transaction boundaries.** A method option `transaction: TransactionOption` (mirroring Orleans:
   `Create`, `CreateOrJoin`, `Join`, `Supported`, `NotAllowed`, `Suppress`) declares how a call
   relates to the ambient transaction. `Create`/`CreateOrJoin` start one if absent; `Join` requires
   one. The proxy/dispatcher begins or propagates a **transaction context** (a transaction id +
   participant set) through the **request context** — the same channel that already carries the
   reentrancy/chain id ([04](../04-messaging-and-serialization.md)), so a transaction flows across
   silos with the call.

2. **Transactional state facet.** A `TransactionalState<T>` facet (alongside `@persistentState`)
   holds versioned state and is accessed only as `performUpdate(fn)` / `performRead(fn)` within a
   transaction. It keeps a **stable committed version** plus per-transaction **tentative** writes,
   and records the read/written version for conflict detection. Outside a transaction, reads see the
   committed version.

3. **Transaction manager + agent.** A per-silo **transaction agent** begins, commits and aborts
   transactions on behalf of the originating call; a **transaction manager** (TM) coordinates the
   commit across the participants enlisted in the transaction. The TM role is co-located with the
   transaction (no central bottleneck), as in Orleans.

4. **Optimistic, serializable commit.** On commit the TM runs **prepare** across participants — each
   validates that the versions it read/wrote are still current (serializable: no intervening
   committed write invalidated them) and durably stages its tentative state and a prepare record —
   then **commit** (make tentative state the committed version) or, on any prepare failure or
   conflict, **abort** (discard tentative state at every participant). Write–write and read–write
   conflicts abort and the caller may retry.

5. **Durability and recovery.** Committed transactional state and the TM's commit record are durable
   (Redis by default, via the existing provider seam). A silo failure mid-commit leaves in-doubt
   participants; on restart the TM (or its successor) resolves each from the commit record so the
   outcome is consistent — committed or aborted, never torn.

## Consequences

- A new storage contract distinct from `PersistentState`: versioned state with tentative writes and a
  commit log, not a single etag record. The two facets coexist on a grain.
- Commit adds round trips (prepare + commit) and cross-silo hops for remote participants — the cost
  of distributed ACID; only transactional calls pay it.
- At-least-once commit application must be idempotent at each participant (recovery may re-apply).
- Serializable isolation means contended transactions abort and retry; callers handle retry, as in
  Orleans.

## Alternatives considered

- **Deterministic transactions (Snapper-style).** Higher throughput under contention, but requires
  pre-declared access sets and a batching layer — a larger departure from the Orleans programming
  model we are matching. Could layer on later behind the same facet.
- **Sagas / compensation.** Eventual, not ACID; pushes correctness onto application code. Not parity.
- **Single-grain only (status quo).** Insufficient — the defining feature is multi-grain atomicity.

## Implementation slices

1. **Transaction context + boundaries.** `TransactionOption` on method options; the proxy/dispatcher
   begins/joins a transaction and propagates the context through the request context across silos. An
   in-memory TM, single silo, no durability. Failing test first: two grains updated in one
   transaction, an induced failure aborts both (no half-apply).
2. **Transactional state facet.** `TransactionalState<T>` with versioned state, per-transaction
   tentative writes, and read/write version tracking; `performUpdate` / `performRead`. Sociable tests
   over the in-memory path.
3. **Optimistic serializable commit.** TM prepare/commit/abort across participants; serializable
   conflict detection; cascading abort. End-to-end: a transfer across two account grains is atomic,
   concurrent transfers serialize, and a conflicting transaction aborts.
4. **Durability + recovery.** Persist committed transactional state and commit records (Redis);
   resolve in-doubt transactions on restart; remote participants over the dispatcher. Multi-silo
   end-to-end: a silo dies mid-commit and the outcome stays consistent.
