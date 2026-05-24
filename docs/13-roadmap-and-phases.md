# 13 — Roadmap and phases

Implementation order, with explicit scope and testable exit criteria per phase. Each phase builds on
the previous and ends in something demonstrable. v1 is the union of phases 1–6.

## v1 scope

**In:** core actor model, persistence, timers, reminders, event streams, Kubernetes hosting.
**Out (deferred):** cross-grain ACID transactions, multi-cluster/geo, grain-interface versioning for
incompatible rolling upgrades, implicit stream subscriptions. See
[01 — non-goals](01-overview-and-goals.md).

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
in-memory and **Redis (default)** providers; Postgres provider.

- Deliverables: `persistence` with memory/redis/postgres providers; runtime read-on-activate wiring.
- **Exit criteria:**
  - State written by a grain survives deactivation and pod restart (Redis).
  - A conflicting write (stale etag) raises `InconsistentStateError`.
  - Multiple named states can target different providers.
  - In-memory provider supports fast sociable tests.

## Phase 5 — Timers and reminders

In-memory timers; durable reminders with the `ReminderTable` contract, hash-range ownership, and
rebalancing; **Redis (default)** + Postgres + in-memory tables.

- Deliverables: timers in `runtime`; `reminders` with memory/redis/postgres tables.
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

## Post-v1 (deferred)

- **Implicit stream subscriptions** (bind a grain type to a namespace, auto-subscribe by key).
- **Directory range handoff** replacing the phase-2 drop-and-rebuild (versioned, lossless handoff
  per [06](06-grain-directory-and-placement.md)).
- **Cross-grain ACID transactions.**
- **Grain-interface versioning** for incompatible rolling upgrades.
- **Multi-cluster / geo-distribution.**
- **Additional providers** (other databases; other stream backings/queue adapters).

## Cross-cutting, throughout

- OpenTelemetry traces (propagated via request context, [04](04-messaging-and-serialization.md)),
  metrics (activations, turn latency, directory hit rate, reminder/stream lag) and structured logs.
- The injectable clock and deterministic placement test aids from
  [12](12-project-structure-and-tooling.md) land in phase 1 and are maintained as features are added.
