# 13 — Roadmap and phases

Implementation order, with explicit scope and testable exit criteria per phase. Each phase builds on
the previous and ends in something demonstrable. The target is **parity with Orleans 10** (see
[01](01-overview-and-goals.md)); this is a rolling roadmap, not a single versioned release.

> Orleans references (parity targets for remaining work):
> `Orleans.Core.Abstractions/Core/IGrainCallFilter.cs` (grain call filters),
> `Orleans.Core.Abstractions/Placement/PlacementFilterStrategy.cs` (placement filters),
> `Orleans.Serialization/Versioning/*` (interface versioning),
> `Orleans.Runtime/Diagnostics/ActivityPropagationGrainCallFilter.cs` (trace propagation);
> and the Orleans-10 subsystems this roadmap predated: grain migration
> (`IGrainMigrationParticipant`, `MigrateOnIdle`), the activation rebalancer
> (`Orleans.Runtime/Placement/Rebalancing/*`, `IActivationRebalancer`), durable journaling
> (`Orleans.Journaling/*`, `DurableGrain`), durable jobs (`Orleans.DurableJobs/*`), and broadcast
> channels (`Orleans.BroadcastChannel/*`).

## Status

**Shipped and verified (phases 1–7):** the core actor model, persistence, timers and reminders, and
event streams — all on **Redis** (the default durable backend) — plus Kubernetes hosting (membership
from EndpointSlices, health, drain, and a cluster e2e). Reducer grains and the
external client also ship. **Cross-grain ACID transactions** ([ADR 0008](adr/0008-cross-grain-transactions.md))
ship too: declarative boundaries, a `TransactionalState<T>` facet with wait-die locking, an
optimistic two-phase commit (TM elected from the writers), durable storage on memory + Redis,
cross-silo participants, and in-doubt recovery. Authoring is **functional by default** —
`defineGrain` + hooks and the `defineReducerGrain` dispatch grain
([ADR 0009](adr/0009-functional-grains.md), [ADR 0010](adr/0010-message-dispatch-reducer-grains.md))
— over the retained class substrate.

Grain call filters and cross-cutting observability (request context, OpenTelemetry traces/metrics,
structured logs) ship too, on the grain-call-filter seam, as does implicit stream subscription.

**Remaining for parity (Orleans 10):** grain migration and the activation rebalancer (the v10
core-runtime block) and placement filters. The Orleans-10 additions of durable journaling
(`DurableGrain`) and durable jobs need an ADR each (journaling overlaps the existing
reducer/persistent-state model).

**Deferred:** additional stream backings — alternatives to the shipped Redis defaults, not parity
gaps. (Postgres grain storage and the Postgres reminder table now ship alongside Redis.)

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
in-memory, **Redis (default)** and Postgres providers.

- Deliverables: `persistence` with memory/redis/postgres providers; runtime read-on-activate wiring.
- **Exit criteria:**
  - State written by a grain survives deactivation and pod restart (Redis).
  - A conflicting write (stale etag) raises `InconsistentStateError`.
  - Multiple named states can target different providers.
  - In-memory provider supports fast sociable tests.

## Phase 5 — Timers and reminders

In-memory timers; durable reminders with the `ReminderTable` contract, hash-range ownership, and
rebalancing; **Redis (default)**, Postgres, and in-memory tables.

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

## Phase 7 — Cross-grain transactions (shipped)

ACID transactions spanning any number of grains: `transaction:` method options, a
`TransactionalState<T>` facet, a transaction manager/agent, and an optimistic, serializable commit
protocol with recovery. Designed in [ADR 0008](adr/0008-cross-grain-transactions.md); shipped and
verified.

- **Exit criteria (all met):**
  - A multi-grain transaction commits atomically; a failure in any participant aborts all of them
    (a transfer between two account grains never half-applies).
  - Isolation is serializable: concurrent transactions do not observe each other's uncommitted state
    (timestamp-ordered wait-die locking; the younger of two contenders aborts).
  - Committed state is durable and survives a participant's deactivation or silo restart (memory +
    Redis transactional storage).
  - A silo failure mid-commit recovers to a consistent outcome (commit or abort, not torn): the TM
    records the commit before participants commit, and in-doubt resources resolve against it on
    activation.

## Remaining for parity (Orleans 10)

- **Grain migration** — live migration of an activation to another silo with its state preserved,
  via `IGrainMigrationParticipant` / `MigrateOnIdle` and directed placement (Orleans 10).
- **Activation rebalancer** — proactively moves activations across silos to balance load
  (`IActivationRebalancer`, `Orleans.Runtime/Placement/Rebalancing/*`; Orleans 10).
- **Grain-interface versioning** — multiple interface versions live at once for heterogeneous rolling
  upgrades, with version-aware placement (Orleans' versioning). **Shipped**
  ([ADR 0014](adr/0014-grain-interface-versioning.md)): an interface carries a `version`; each silo
  advertises a grain manifest exchanged lazily across the cluster; placement pre-filters candidates by
  a compatibility director (`backwardCompatible` / `strict`) and version selector (`latest` / `all` /
  `minimum`), configured via `createSilo().useVersioning(...)`, with best-effort fallback when no silo
  is compatible. Inert in a v1-only cluster.
- **Implicit stream subscriptions** — bind a grain type to a namespace and auto-subscribe by key,
  with no explicit `subscribe` call (Orleans' `[ImplicitStreamSubscription]`). **Shipped** —
  `@implicitStreamSubscription(namespace)` / `defineGrain`'s `implicitSubscriptions`; the grain
  exposes a handler under `STREAM_SUBSCRIPTION_OBSERVER` and the pulling agent fans each event out to
  implicit subscribers (by key) alongside explicit ones (see [09](09-event-streams.md)).
- **Directory range handoff** — replace the phase-2 drop-and-rebuild with a versioned, lossless
  handoff on membership change (per [06](06-grain-directory-and-placement.md)). **Shipped** — on a
  view change the silo losing a range sets its live entries aside and the new owner recovers them by
  pulling from the previous owner; directory ops carry the membership view version (a behind owner
  catches up, a stale caller is redirected via a `staleView` rejection), and reads for a
  still-recovering range wait, so a join no longer reactivates the moved grains.
- **Grain call filters** — incoming/outgoing interception around grain calls for cross-cutting
  concerns (auth, retries, trace propagation), mirroring Orleans' `IIncomingGrainCallFilter` /
  `IOutgoingGrainCallFilter` ([ADR 0012](adr/0012-grain-call-filters.md)). This is also the seam the
  observability work below plugs into. **Shipped** — incoming and outgoing filters (silo-wide, via
  `addIncomingCallFilter` / `addOutgoingCallFilter`) and per-grain filters (a grain filters its own
  calls via the `INCOMING_CALL_FILTER` symbol).
- **Placement filters** — **Shipped**. A `PlacementFilter` layer (Orleans' `PlacementFilterStrategy`)
  prunes the candidate silo set by metadata before a placement strategy runs (`MetadataMatchFilter`),
  alongside the `SiloRoleBasedPlacement` and `ResourceOptimizedPlacement` strategies; silo metadata
  rides the membership snapshot (see [06](06-grain-directory-and-placement.md)). Follow-ups: derive
  silo metadata from Kubernetes pod labels, and report remote silo load via membership gossip (today
  a peer's resource stats are zero/metadata-only).
- **Broadcast channels** — **Shipped** ([ADR 0015](adr/0015-broadcast-channels.md)). Direct in-cluster
  pub/sub without the pulling-agent / cursor machinery (`Orleans.BroadcastChannel/*`,
  `IBroadcastChannelProvider`): a publish fans the item out over the dispatcher to the channel's
  implicit subscribers (`@implicitChannelSubscription`), each receiving it through a
  `BROADCAST_CHANNEL_OBSERVER` handler. Configured with `createSilo(...).useBroadcastChannels(name)`;
  delivery awaits subscribers (one deliberate divergence from Orleans' fire-and-forget default).
- **Durable journaling (`DurableGrain`)** — needs an ADR: Orleans 10's `Orleans.Journaling`
  (`DurableValue`/`DurableDictionary`/`DurableList`/… that journal mutations automatically) overlaps
  the existing reducer/persistent-state model; decide whether to adopt it or map it onto what ships.
- **Durable jobs** — needs an ADR: Orleans 10's `Orleans.DurableJobs` (durable execution / workflow
  engine) is a large, optional addition.

## Deferred (not parity gaps)

- **Additional providers** — other databases and stream backings/queue adapters. Redis is the
  shipped default; Postgres grain storage and reminder table also ship; these are alternatives.

## Beyond parity (post-Orleans-10 directions)

Directions to pursue once Orleans 10 parity is achieved. These are **extensions beyond Orleans**, not
parity items, and each warrants its own ADR before implementation.

- **Browser state replication and browser-hosted grains.** Replicate grain state to the browser as a
  live read-view, and eventually run a subset of grains in a lightweight browser-side runtime, with a
  **server-enforced** policy for which grain types are permitted there. Three layers of ambition:
  1. _State replication / live read-views_ — the server stays the source of truth and the browser
     holds a derived, live replica, built on the external client ([11](11-public-api-and-examples.md))
     and event streams ([09](09-event-streams.md)) (the analogue of Orleans grain observers).
  2. _Browser-hosted grains_ — a partial silo hosting some activations client-side and forwarding the
     rest; the catalog, scheduler and facet machinery are already host- and transport-agnostic.
  3. _Permission model (the crux)_ — "permitted to run there" is a **trust/authority classification**,
     not merely placement, because the browser is untrusted: pair a grain-type marker (a
     client-placement capability) with a gate **enforced on the silo**, never self-granted by the
     client. Per-user / view-model / optimistic-UI state may live client-side; **shared, authoritative
     or secret state must not** — the transactional grains of [ADR 0008](adr/0008-cross-grain-transactions.md)
     in particular assume a trusted single activation (wait-die locking, durable commit), so a browser
     replica of authoritative state needs a different consistency model (optimistic / CRDT with
     server reconciliation). The intended motivation (offline, optimistic-UI latency, or reduced
     server load) drives the design and should be settled in the ADR first.

## Cross-cutting, throughout

- OpenTelemetry traces, metrics (activations, turn latency, directory hit rate, reminder/stream lag)
  and structured logs. Faithful to Orleans, tracing is a **grain call filter** that injects/extracts
  W3C trace context through the ambient request context ([04](04-messaging-and-serialization.md)) —
  the analogue of Orleans' `ActivityPropagationGrainCallFilter` — so spans stitch across grain calls
  and silos. **Shipped** ([ADR 0013](adr/0013-observability.md)): the **grain-call-filter seam**
  ([ADR 0012](adr/0012-grain-call-filters.md)), the **ambient request context** (`requestContext.get/set`,
  propagated in-process and across silos), **OpenTelemetry tracing** (`createSilo().useTracing()` —
  CLIENT/SERVER spans with W3C propagation), **metrics** (`useMetrics()` — a grain-call counter +
  duration histogram, an activation-count gauge, and directory-cache hit/miss counters), and
  **structured logging** (`useLogging()` — a `Logger` seam + a call-logging filter), in
  `@tsva/observability` and no-op without an SDK/logger. Reminder/stream-lag gauges are deferred as
  optional polish.
- The injectable clock and deterministic placement test aids from
  [12](12-project-structure-and-tooling.md) land in phase 1 and are maintained as features are added.
