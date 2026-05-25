# 13 — Roadmap and phases

Implementation order, with explicit scope and testable exit criteria per phase. Each phase builds on
the previous and ends in something demonstrable. The target is **Orleans parity** (see
[01](01-overview-and-goals.md)); this is a rolling roadmap, not a single versioned release.

> Orleans references (parity targets for remaining work): `Orleans.Transactions/*` (transactions),
> `Orleans.Core.Abstractions/Core/IGrainCallFilter.cs` (grain call filters),
> `Orleans.Core.Abstractions/Placement/PlacementFilterStrategy.cs` (placement filters),
> `Orleans.Serialization/Versioning/*` (interface versioning),
> `Orleans.Runtime/Diagnostics/ActivityPropagationGrainCallFilter.cs` (trace propagation).

## Status

**Shipped and verified (phases 1–6):** the core actor model, persistence, timers and reminders, and
event streams — all on **Redis** (the default durable backend) — plus Kubernetes hosting (membership
from EndpointSlices, health, drain, and a cluster e2e). Reducer grains (snapshot mode) and the
external client also ship. Authoring is **functional by default** — `defineGrain` + hooks and the
`defineReducerGrain` dispatch grain ([ADR 0009](adr/0009-functional-grains.md),
[ADR 0010](adr/0010-message-dispatch-reducer-grains.md)) — over the retained class substrate.

**Remaining for parity:** cross-grain ACID transactions
([ADR 0008](adr/0008-cross-grain-transactions.md)) — the next priority — then grain-interface
versioning, implicit stream subscriptions, and lossless directory range handoff. Cross-cutting
observability (OpenTelemetry traces/metrics, structured logs) remains to be wired throughout.

**Out of scope / deferred:** multi-cluster/geo (Orleans removed it in 3.0), the reducer event-log
mode, and additional providers (Postgres storage/reminders, other stream backings) — alternatives to
the shipped Redis defaults, not parity gaps.

## Phase 1 — Single-silo core actor model

Grains, identity, proxy references, decorators, turn scheduler, catalog, activation lifecycle, the
grain factory. No networking yet — everything in one process.

- Deliverables: `core`, the turn scheduler and catalog in `runtime`, the `Proxy`-based references,
  `@grain` / `defineGrainInterface`.
- **Exit criteria:**
  - A grain activates on first call and runs `onActivate` before the first message.
  - Concurrent calls to one grain execute as serialized turns; a `readOnly`/`@reentrant` method
    interleaves; a non-reentrant call does not.
  - Idle deactivation runs `onDeactivate`; the next call reactivates.
  - Sociable tests (see [12](12-project-structure-and-tooling.md)) cover the above with a fake clock.

## Phase 2 — Messaging and multi-silo

WebSocket transport, message envelope, serializer, dispatcher, correlation table; the grain
directory (DHT ring) and placement; static membership.

- Deliverables: `messaging`, `directory`, placement in `runtime`, static-membership provider.
- **Exit criteria:**
  - Two+ silos in one process over real WebSocket transport route calls across silos.
  - Directory `register` is compare-and-set: a forced activation race yields exactly one winner; the
    loser forwards.
  - Location cache hit avoids a directory round-trip; a stale cache entry is invalidated and
    re-resolved on rejection.
  - Removing a silo from the static view rebalances ring ownership and reactivates affected grains on
    next call.
  - Random, prefer-local, activation-count and stateless-worker placement each behave as specified.

## Phase 3 — Kubernetes membership and hosting

Replace static membership with the Kubernetes EndpointSlice watch; health endpoints; the hosting
builder; reference manifests; graceful drain on `SIGTERM`.

- Deliverables: `clustering-k8s`, `hosting`, manifests, health endpoints.
- **Exit criteria:**
  - A `StatefulSet` of 3 silos forms a cluster in kind; the membership view matches ready endpoints.
  - Killing a pod removes it from membership; its grains reactivate elsewhere on next call.
  - A rolling update drains pods one at a time with no lost calls beyond expected retries.
  - `/ready`, `/live`, `/startup` behave correctly through join, drain and overload.

## Phase 4 — Persistence

`PersistentState` facet, `@persistentState`, the storage provider contract, etag concurrency;
in-memory and **Redis (default)** providers. (A Postgres provider is deferred as an additional
provider — not a parity gap.)

- Deliverables: `persistence` with memory/redis providers; runtime read-on-activate wiring.
- **Exit criteria:**
  - State written by a grain survives deactivation and pod restart (Redis).
  - A conflicting write (stale etag) raises `InconsistentStateError`.
  - Multiple named states can target different providers.
  - In-memory provider supports fast sociable tests.

## Phase 5 — Timers and reminders

In-memory timers; durable reminders with the `ReminderTable` contract, hash-range ownership, and
rebalancing; **Redis (default)** + in-memory tables. (A Postgres table is deferred as an additional
provider — not a parity gap.)

- Deliverables: timers in `runtime`; `reminders` with memory/redis tables.
- **Exit criteria:**
  - A timer fires as a turn and is cancelled on deactivation.
  - A reminder fires after the grain has been deactivated, reactivating it.
  - A reminder survives the owning silo's death: another silo picks up its hash range and fires it.
  - Reminder ticks respect single-threaded execution.

## Phase 6 — Event streams

`StreamProvider` / `AsyncStream` / subscription handles; pulling agents over physical queues; queue
ownership via the ring; durable cursors; at-least-once delivery; **Redis Streams (default)** +
in-memory providers.

- Deliverables: `streams` with redis-streams/memory providers; pulling agents and balancer in
  `runtime`.
- **Exit criteria:**
  - A producer grain publishes; a consumer grain receives events in publish order, delivered as
    turns.
  - A subscription is durable: after the consumer deactivates and reactivates, it resumes from its
    cursor.
  - Queue ownership rebalances on membership change; a new owner resumes from the committed cursor
    with at-least-once delivery (no gaps, possible idempotent redelivery).
  - The worked thermostat example ([11](11-public-api-and-examples.md)) runs end-to-end on kind.

## Phase 7 — Cross-grain transactions

ACID transactions spanning any number of grains: `[transaction]` method options, an
`ITransactionalState`-style facet, a transaction manager/agent, and an optimistic, serializable
commit protocol with recovery. The next priority; designed in
[ADR 0008](adr/0008-cross-grain-transactions.md).

- **Exit criteria:**
  - A multi-grain transaction commits atomically; a failure in any participant aborts all of them
    (a transfer between two account grains never half-applies).
  - Isolation is serializable: concurrent transactions do not observe each other's uncommitted state.
  - Committed state is durable and survives a participant's deactivation or silo restart.
  - A silo failure mid-commit recovers to a consistent outcome (commit or abort, not torn).

## Remaining for parity (after transactions)

- **Grain-interface versioning** — multiple interface versions live at once for heterogeneous rolling
  upgrades, with version-aware placement (Orleans' versioning).
- **Implicit stream subscriptions** — bind a grain type to a namespace and auto-subscribe by key,
  with no explicit `subscribe` call (Orleans' `[ImplicitStreamSubscription]`).
- **Directory range handoff** — replace the phase-2 drop-and-rebuild with a versioned, lossless
  handoff on membership change (per [06](06-grain-directory-and-placement.md)).
- **Grain call filters** — incoming/outgoing interception around grain calls for cross-cutting
  concerns (auth, retries, trace propagation), mirroring Orleans' `IIncomingGrainCallFilter` /
  `IOutgoingGrainCallFilter`. This is also the seam the observability work below plugs into.
- **Placement filters** — prune the candidate silo set by metadata before a placement strategy runs
  (Orleans' `PlacementFilterStrategy`); pairs with the additional placement strategies
  (`SiloRoleBasedPlacement`, `ResourceOptimizedPlacement`) noted in
  [06](06-grain-directory-and-placement.md).

## Deferred (not parity gaps)

- **Multi-cluster / geo-distribution** — out of scope: Orleans removed it in 3.0.
- **Additional providers** — Postgres grain storage and reminder table, plus other databases and
  stream backings/queue adapters. Redis is the shipped default; these are alternatives.
- **Reducer event-log mode** — the reducer programming model ([ADR 0006](adr/0006-reducer-grains.md))
  ships in snapshot mode (events transient); the append-only event-log persistence mode is deferred.
  This is an addition beyond Orleans, not a parity item.

## Cross-cutting, throughout

- OpenTelemetry traces, metrics (activations, turn latency, directory hit rate, reminder/stream lag)
  and structured logs. Faithful to Orleans, tracing is a **grain call filter** that injects/extracts
  W3C trace context through the ambient request context ([04](04-messaging-and-serialization.md)) —
  the analogue of Orleans' `ActivityPropagationGrainCallFilter` — so spans stitch across grain calls
  and silos. This depends on the grain-call-filter seam listed above.
- The injectable clock and deterministic placement test aids from
  [12](12-project-structure-and-tooling.md) land in phase 1 and are maintained as features are added.
