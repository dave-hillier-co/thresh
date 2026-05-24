# 07 — Persistence

Grains keep state in memory while active, but that state must survive deactivation and pod loss.
Persistence is the durable home for grain state. Unlike the grain directory (which is ephemeral and
in-silo, see [06](06-grain-directory-and-placement.md)), persistence uses an **external durable
store**, defaulting to **Redis**.

> Orleans references: `Orleans.Core/Providers/IGrainStorage.cs`,
> `Orleans.Core.Abstractions/Core/IStorage.cs`,
> `Orleans.Runtime/Facet/Persistent/IPersistentState.cs`,
> `Orleans.Core/CodeGeneration/IGrainState.cs`,
> `Orleans.Persistence.Memory/Storage/MemoryStorage.cs`.

## The model

A grain declares one or more named persistent state objects. Each is read on activation (or lazily)
and written explicitly by the grain. Reads and writes go through a **storage provider**. State is
kept in memory between writes so reads are served without touching the store, exactly as in Orleans.

```ts
@grain()
class AccountGrain extends Grain implements IAccount {
  @persistentState("balance")
  private balance!: PersistentState<BalanceState>;

  async deposit(amount: number): Promise<void> {
    this.balance.value.cents += amount;
    await this.balance.write();   // durably persist
  }

  async getBalance(): Promise<number> {
    return this.balance.value.cents;   // served from memory, no store hit
  }
}
```

A grain may have several named states stored in different providers — e.g. `"profile"` in one store
and `"inventory"` in another — matching Orleans' multiple-named-state capability.

## Interfaces

The grain-facing facet, mirroring Orleans `IPersistentState<T>` / `IStorage<T>`:

```ts
interface PersistentState<TState> {
  value: TState;             // the in-memory state (mutable)
  readonly etag?: string;    // optimistic-concurrency token
  readonly exists: boolean;  // whether a record exists in the store

  read(): Promise<void>;     // load from store into value
  write(): Promise<void>;    // persist value; bumps etag
  clear(): Promise<void>;    // delete the record
}
```

The provider contract, mirroring Orleans `IGrainStorage`:

```ts
interface GrainStorage {
  read<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
  write<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
  clear<T>(stateName: string, grainId: GrainId, state: StateHolder<T>): Promise<void>;
}

interface StateHolder<T> {
  value: T;
  etag?: string;
  exists: boolean;
}
```

The provider owns serialization (via the shared `Serializer`, see
[04](04-messaging-and-serialization.md)), the storage mechanics, and etag handling. The runtime owns
when to call it.

## Optimistic concurrency with etags

Each write carries the etag the grain last read. The provider performs a conditional write: if the
stored etag differs, the write is rejected and the runtime raises an `InconsistentStateError`
(Orleans: `InconsistentStateException`). This protects against two incarnations of the same grain
(e.g. a brief split-brain, see [05](05-clustering-membership-k8s.md)) clobbering each other's state:
the stale writer fails and can re-read and retry.

```mermaid
sequenceDiagram
    participant G as Grain
    participant S as GrainStorage (Redis)
    G->>S: write(value, etag=v3)
    alt stored etag == v3
        S-->>G: ok, new etag=v4
    else stored etag != v3
        S-->>G: conflict
        G->>G: throw InconsistentStateError
    end
```

## Activation integration

- **Read on activate (default).** The runtime reads declared states before the first message, so
  `onActivate` and the first method see populated `value`. A grain can opt for lazy reads if startup
  cost matters.
- **Write is explicit.** Nothing is persisted until the grain calls `write()`. This keeps the
  durability boundary visible in the code (Orleans takes the same stance).
- **Flush on deactivate is the grain's choice.** A grain that wants last-moment durability writes in
  `onDeactivate`; the runtime awaits it during graceful shutdown (see [03](03-runtime-and-silo.md)).

## Providers

The provider is selected per state name via configuration, with a default provider for unspecified
states. Redis is the default; see [ADR 0005](adr/0005-redis-default-providers.md) for why.

| Provider | Use | Notes |
| --- | --- | --- |
| **Redis (default)** | Production default | State stored as a serialized value per `(grainType, key, stateName)`; etag via Redis `WATCH`/`MULTI` or a Lua compare-and-set; optional TTL for cache-like grains. |
| **Postgres** | Relational alternative | One row per state, etag column, conditional `UPDATE ... WHERE etag = $1`. Good when state must be queried/reported on outside the actor model. |
| **In-memory** | Dev and tests only | A `Map` in the silo process; not durable, not shared across silos. Mirrors Orleans `MemoryGrainStorage`. |

Selecting and configuring providers is part of the hosting builder
(see [11 — Public API](11-public-api-and-examples.md)):

```ts
silo
  .addRedisStorage("default", { url: process.env.REDIS_URL })
  .addPostgresStorage("reporting", { connectionString: process.env.PG_URL });
```

A grain then names a non-default provider with `@persistentState("ledger", { provider: "reporting" })`.

## What persistence does not do

- It does not give cross-grain ACID transactions (that is a deferred feature — see
  [01 non-goals](01-overview-and-goals.md)).
- It does not automatically write on every field mutation; durability is explicit via `write()`.
- It does not replace the directory; a grain's *location* is ephemeral and never persisted here.
