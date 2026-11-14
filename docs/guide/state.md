# State and transactions

State in ordinary closure variables exists only for one activation. Choose durability deliberately.

Every facet below is a **hook**: it resolves the activation being set up from an ambient slot that is
live only during the *synchronous* body of the factory. Call hooks at the top level of the factory,
never after an `await`, from a method body, or from `onActivate` — those throw an error naming the
hook and the rule. To reach the runtime after setup, capture the factory's `ctx` parameter in the
closure and use `ctx.runtime` / `ctx.id` / `ctx.getGrain` from method bodies.

## Persistent and reducer state

`usePersistentState<T>(name, options)` exposes `value` plus explicit `read`, `write`, and
`clear` operations. Configure its named provider with `useMemoryStorage`, `addRedisStorage`, or
`addPostgresStorage`. Memory providers are only for examples and tests. Storage uses optimistic
versioning; do not silently overwrite inconsistent-state failures.

`useReducerState` applies a pure `(state, event) => state` reducer and persists its snapshot.
`defineReducerGrain` can expose `dispatch(action)` and `query()` and return Elm-style effects for
cross-grain work. Keep reducers deterministic and free of I/O.

## Transactions and journals

`useTransactionalState` supplies transactional operations. Mark interface methods with transaction
options and configure a transactional storage provider. Transactions coordinate participating
grain state; keep them short and avoid unrelated external side effects.

Journaling stores events, periodically snapshots state, and truncates the log. `useDurableState`,
`useDurableDictionary`, `useDurableList`, `useDurableQueue`, and `useDurableSet` expose journaled
collections. Configure memory, Redis or Postgres journaling (`addPostgresJournaling`) under the same
provider name used by the grain.

Compaction applies to durable collections: a grain that uses only those truncates its log to a
snapshot at the threshold (`snapshotThreshold`, default 100 entries). A `JournaledGrain` shares that
log but its events are history, not just state, so a snapshot never discards them by default: the
snapshot lets activation start from the confirmed view without folding every event through
`transitionState`, and `retrieveConfirmedEvents` still serves the full range, before and after
re-activation. Letting a snapshot replace a `JournaledGrain`'s events is an explicit choice
(`retainJournaledEvents: false` in the silo config); after it, a range starting below the latest
snapshot's version throws. Retained events are held in memory and rewritten at each compaction, so
a grain that raises events without bound pays for its history in both.

A conditional raise (`raiseConditionalEvent`) that returns `false` has lost a race to another
writer. Before it returns, the confirmed state and version are refreshed from storage, so a retry
computed from them can succeed. This holds for the journal substrate and for custom-storage grains.

Providers registered under different names over the same backing store (the same Postgres pool,
tables and service id, or the same Redis client, prefix and service id) share one log per grain;
the provider name is not part of a storage key, and a grain's `JournaledGrain` events and durable
fields over such providers run under one manager.

## Choosing where state lives

An application should name its providers and leave the database to the host. `useStorageBackend`
takes one backend — memory, Postgres or Redis — and uses it for every grain-storage and journal
provider name the silo does not register explicitly, and for the reminder table when
`useReminders()` names none. `storageBackendFromEnvironment()` reads it from `THRESH_STORAGE_URL`
(`postgres://…` or `redis://…`; unset means memory), so a deployment changes backend without code:

```ts
createSilo(config)
  .useStorageBackend(storageBackendFromEnvironment())
  .useReminders()
  .registerGrain(/* … */);
```

A Postgres backend shares one pool across its `thresh_grain_state`, `thresh_journal_*` and
`thresh_reminders` tables, created on start. In-memory storage is refused at build for anything but
a static view of the silo alone, because each silo would hold its own store and a grain reactivating
elsewhere would start empty; set `THRESH_STORAGE_ALLOW_IN_MEMORY_CLUSTER=true` only for a cluster
whose state is disposable — that opt-in admits the backend's per-silo reminder table too, so a
reminder registered on a silo that does not own its grain's range never fires. A blank
`THRESH_STORAGE_URL` is refused rather than read as memory.

The same rule holds for providers registered by hand. A silo whose membership is anything but a
static view of itself alone refuses, at build, an in-memory grain store, journal store or reminder
table: a grain reactivating on another silo would read an empty store, and a reminder fires on the
silo owning its grain's range, reading that silo's table. A store the builder makes itself
(`useMemoryStorage()`, `useMemoryJournaling()` with no argument, `useReminders()` with neither table
nor backend) is one per silo and is always refused there. An instance handed in is accepted only
under `allowSharedInMemoryState()`, which declares that every silo of the cluster is handed that same
instance in one process — what `TestCluster` does.
