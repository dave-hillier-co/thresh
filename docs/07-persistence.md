# 07 — Persistence

Grains keep state in memory while active; persistence is its durable home, surviving deactivation and
pod loss. Unlike the ephemeral in-silo directory ([06](06-grain-directory-and-placement.md)),
persistence uses an **external durable store**, defaulting to **Redis**.

> Orleans references: `Orleans.Core/Providers/IGrainStorage.cs`,
> `Orleans.Core.Abstractions/Core/IStorage.cs`,
> `Orleans.Runtime/Facet/Persistent/IPersistentState.cs`,
> `Orleans.Persistence.Memory/Storage/MemoryStorage.cs`.

## The model

A grain declares named persistent state, read on activation and written explicitly through a storage
provider; state stays in memory between writes so reads don't touch the store (as in Orleans). A grain
may have several named states in different providers.

```ts
const AccountGrain = defineGrain<IAccount>("Account", (ctx) => {
  const balance = usePersistentState<BalanceState>(ctx, "balance");
  return {
    deposit: async (amount) => { balance.value.cents += amount; await balance.write(); },
    getBalance: async () => balance.value.cents, // served from memory
  };
});
```

The class form uses the `@persistentState("balance")` decorator that `usePersistentState` wraps.

## Interfaces

The grain-facing facet mirrors Orleans `IPersistentState<T>` / `IStorage<T>`; the provider contract
mirrors `IGrainStorage`:

```ts
interface PersistentState<TState> {
  value: TState;            // in-memory state (mutable)
  readonly etag?: string;   // optimistic-concurrency token
  readonly exists: boolean;
  read(): Promise<void>; write(): Promise<void>; clear(): Promise<void>;
}
interface GrainStorage {
  read<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
  write<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
  clear<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
}
```

The provider owns serialization (via the shared `Serializer`, [04](04-messaging-and-serialization.md)),
storage mechanics, and etags; the runtime owns when to call it.

## Optimistic concurrency with etags

Each write carries the etag last read; the provider does a conditional write and, on a mismatch, the
runtime raises `InconsistentStateError` (Orleans' `InconsistentStateException`). This stops two
incarnations of a grain (e.g. a brief split-brain, [05](05-clustering-membership-k8s.md)) from
clobbering each other — the stale writer fails and re-reads. An absent etag means an unconditional
create (Orleans' null-etag sentinel).

## Activation integration

- **Read on activate (default)** — the runtime reads declared states before the first message, so
  `onActivate` and the first method see populated `value`. `usePersistentState(ctx, name, { defaultValue })`
  (or the `@persistentState` decorator) registers the facet; a catalog hook injects it (bound to the
  grain id and its provider) and reads it before `onActivate`. The default provider is `"default"`.
- **Write is explicit** — nothing persists until `write()`, keeping the durability boundary visible.
- **Flush on deactivate is the grain's choice** — write in `onDeactivate`; the runtime awaits it during
  graceful shutdown ([03](03-runtime-and-silo.md)).

## Providers

Selected per state name with a default provider; Redis is the default
([ADR 0005](adr/0005-redis-default-providers.md)).

- **Redis (default)** — a serialized value per `(grainType, key, stateName)`; etag via a Lua
  compare-and-set; optional TTL. `addRedisStorage("default", { url })`.
- **Postgres** — one row per state with an etag column and conditional `UPDATE ... WHERE etag = $1`;
  good when state must be queried outside the actor model. `addPostgresStorage(name, { connectionString })`.
- **In-memory** — dev/tests; a `Map` in-process, not durable (Orleans' `MemoryGrainStorage`).

A grain names a non-default provider via `usePersistentState(ctx, "ledger", { provider: "reporting" })`.

## Reducer grains: an event-routed alternative

The facet above is **mutable** (read, mutate, `write()`). The **reducer** facet
`useReducerState(ctx, name, { initial, reduce })` instead has command handlers `raise` past-tense
events that a pure reducer folds into *immutable* state, persisting the folded snapshot through the
same `GrainStorage` + etag machinery (events are transient). `defineReducerGrain(name, { initial,
reduce })` goes further — the whole grain becomes a `dispatch(action)` + `query()` loop with cross-grain
work as Elm-style effects. See [ADR 0006](adr/0006-reducer-grains.md),
[ADR 0010](adr/0010-message-dispatch-reducer-grains.md), and [`examples/bank`](../examples/bank).

## What persistence does not do

- No cross-grain ACID transactions — the etag write is atomic for a **single grain**; spanning grains
  is the `TransactionalState` facet of [ADR 0008](adr/0008-cross-grain-transactions.md) (the two
  coexist on a grain).
- No automatic write on field mutation — durability is explicit via `write()`.
- No directory replacement — a grain's *location* is ephemeral and never persisted here.
