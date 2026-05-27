# ADR 0019 — Durable journaling (`DurableGrain`)

- Status: Accepted — implemented (`@tsva/journaling`)
- Context docs: [07 — Persistence](../07-persistence.md), [02 — Actor model](../02-actor-model.md),
  [13 — Roadmap](../13-roadmap-and-phases.md), [ADR 0006 — Reducer grains](0006-reducer-grains.md),
  [ADR 0005 — Redis defaults](0005-redis-default-providers.md)

> Orleans references (the model this ADR ports): `Orleans.Journaling/StateMachineManager.cs` and
> `Orleans.Journaling/IStateMachineManager.cs` (the per-grain manager that owns one append-only log,
> registers state machines, replays on activation, and snapshots), `Orleans.Journaling/IDurableStateMachine.cs`
> (`Reset` / `Apply(ReadOnlySequence)` / snapshot), `Orleans.Journaling/{DurableValue,DurableDictionary,DurableList}.cs`
> (the journalled structures), and the log-storage seam `Orleans.Journaling/{ILogStorage,IStateMachineStorage}.cs`.
> Orleans' own prior art for the event-driven shape is `JournaledGrain<TState,TEvent>` ([ADR 0006](0006-reducer-grains.md)
> already ports its *programming model* as the reducer snapshot facet; this ADR ports the *durable log*).

## Context

Two durable models ship today and both deliberately keep the **mutation history transient**:

- **PersistentState** ([07](../07-persistence.md), `@persistentState` / `usePersistentState`) is a
  mutable cell: read, mutate `value`, `write()` the whole value under etag optimistic concurrency. It
  stores the *current value*, never how it got there.
- **The reducer snapshot facet** ([ADR 0006](0006-reducer-grains.md), `@reducerState` /
  `useReducerState`) folds past-tense events through a pure reducer into immutable state and persists
  the **fold as a snapshot**. ADR 0006 is explicit that "the raised events are transient per-turn
  values… this adds no new store" — it ports `JournaledGrain`'s *model* but **not** a durable event log.

Orleans 10's `Orleans.Journaling` is the third path those two left open: a `DurableGrain` whose
`DurableValue<T>` / `DurableDictionary<K,V>` / `DurableList<T>` **journal each mutation to an
append-only log** and **replay the log on activation** to rebuild state, coordinated by an
`IStateMachineManager` over `IDurableStateMachine`s. The durable artefact is the *log of mutations*,
not a whole-value snapshot — periodic snapshots exist only to bound replay cost. The roadmap
([13](../13-roadmap-and-phases.md)) flagged this as a parity gap pending an ADR to settle how it
relates to, and stays separate from, the reducer/persistent facets. This ADR settles it.

## Decision

Add durable journaling as an **additive** facet — a new programming model and a new storage seam —
faithfully porting Orleans' `IStateMachineManager` / `IDurableStateMachine` model. It does **not**
replace or alter PersistentState or the reducer snapshot facet; a grain opts into one model. No change
to `cluster-node.ts` / `activation.ts` beyond a single new call at the existing `stateBinder`
facet-binding seam in `silo-builder.ts`.

1. **Per-grain manager over an append-only log.** A `StateMachineManager` owns **one** append-only log
   per grain (per provider); each durable structure registers as a named `DurableStateMachine`
   (`reset()` / `apply(payload)` / `snapshot()`). On activation the manager reads the log **once** and
   dispatches every entry to its owning machine — the single replay rebuilds all the grain's
   structures. A mutation appends one framed entry (`{ machine, kind, payload }`, serialized through
   the shared value-codec) through the manager, then updates the structure's in-memory state. This is
   the faithful Orleans split: the manager frames/dispatches/persists, the machine interprets payloads.

2. **A separate `JournalStorage` provider seam, same concurrency contract.** Journaling needs
   append + read-log + atomic-replace, not the read/write/clear of a single cell, so it gets its own
   provider interface and its own keyspace (`tsva:journal:…`, distinct from PersistentState's
   `tsva:state:…`) — *not* `GrainStorage`. It keeps the **same optimistic-concurrency contract**: a
   monotonically increasing numeric `version` is the CAS token (the etag analogue), and a conflicting
   write raises the existing `InconsistentStateError`. Memory and Redis providers ship; the Redis one
   reuses the `redis-grain-storage.ts` Lua/CAS pattern (a Redis list of entries plus a version key,
   mutated under one atomic Lua eval).

3. **Snapshotting bounds the log.** When the log crosses a configurable entry threshold on append, the
   manager snapshots every machine and **atomically replaces** (truncates) the log under the same
   version CAS. Replay is uniform — a snapshot frame is applied through the same `apply()` path as an
   incremental op — so a compacted log replays exactly like an uncompacted one.

### Overlap and boundary (explicit)

| | PersistentState ([07](../07-persistence.md)) | Reducer snapshot ([ADR 0006](0006-reducer-grains.md)) | **Durable journaling (this ADR)** |
| --- | --- | --- | --- |
| Durable artefact | whole current value | folded-state snapshot | **append-only log of mutations** (+ periodic snapshot to bound replay) |
| Mutation history | none | transient (events discarded) | **durable** — the log *is* the source of truth |
| Storage seam | `GrainStorage` (`tsva:state:…`) | `GrainStorage` (reused) | **`JournalStorage`** (`tsva:journal:…`) |
| Concurrency | etag CAS | etag CAS | **version CAS** (same `InconsistentStateError`) |
| Rebuild on activate | one read | one read of the snapshot | **replay the log** (after the latest snapshot) |

- **vs PersistentState** — both are single-grain and optimistic-concurrency-guarded, but PersistentState
  rewrites the whole value while journaling appends deltas and replays. Different access pattern ⇒
  different provider and keyspace. PersistentState is unchanged.
- **vs the reducer snapshot facet** — both derive state from an ordered sequence of events/ops. ADR 0006
  keeps the sequence transient and persists only the fold; journaling keeps the sequence **durable** and
  snapshots only to bound replay. Journaling is the durable-log counterpart ADR 0006 deliberately left
  out — not a replacement. A reducer grain that wants a durable event log is expressible as a
  `DurableList` of events plus a `DurableValue` fold, but that is a choice, not a migration.

The single-writer / one-turn-at-a-time model ([02](../02-actor-model.md)) does the hard part, exactly
as it does for the reducer facet: appends and the replay are serialized and lock-free within an
activation; the only concurrency is a cross-silo split-brain incarnation, which the version CAS fences
out (the loser is a zombie that deactivates and re-replays the winner's log) — identical to how
`RedisGrainStorage` guards PersistentState.

## Consequences

- **A new store and provider seam** (`JournalStorage`, memory + Redis) and a new package
  `@tsva/journaling`, mirroring the `@tsva/persistence` / `@tsva/transactions` split; contracts the
  runtime/hosting need live in `@tsva/core` (`journal-storage`, `durable-state-machine`,
  `durable-state`, `durable-state-metadata`).
- **One storage round-trip per mutation** (faithful to Orleans, which persists each op). Read cost is
  bounded by the snapshot threshold, not the lifetime mutation count. Reads of current state are served
  from memory.
- **Mutators are async.** `DurableValue.set` / `DurableDictionary.set` / `DurableList.add` etc. return
  promises (each appends to the log), unlike `@persistentState`'s in-memory mutate-then-`write()`.
- **All durable structures on a grain share one log and one provider** (the manager owns it). A grain
  may still use a non-default journaling provider; mixing providers across structures on one grain is
  rejected.
- **Additive and inert when unused.** Grains that declare no durable fields, and silos that configure no
  journaling provider, are unaffected; the binder is a no-op.

## Alternatives considered

1. **Reuse `GrainStorage` and store the whole log as one value.** Rewriting the entire log on every
   mutation reduces journaling to PersistentState-of-an-array — it loses append semantics, scales
   poorly, and defeats the point. Rejected in favour of an append-only provider.
2. **One log per facet (keyed by `stateName`) instead of a per-grain manager.** Simpler and matches the
   `tsva:state:{grain}/{name}` key scheme, but diverges from Orleans' single-`IStateMachineManager`
   model and loses atomic multi-structure snapshots. Rejected for fidelity.
3. **Fold journaling into the reducer facet (make ADR 0006 events durable).** Conflates two deliberately
   separate models — ADR 0006's value (pure, testable, snapshot-only folds) is independent of
   durability. Kept separate and additive.
