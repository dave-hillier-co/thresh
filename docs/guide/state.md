# State and transactions

State in ordinary closure variables exists only for one activation. Choose durability deliberately.

## Persistent and reducer state

`usePersistentState<T>(ctx, name, options)` exposes `value` plus explicit `read`, `write`, and
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
collections. Configure memory or Redis journaling under the same provider name used by the grain.
