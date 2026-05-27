# 13 — Roadmap and phases

Implementation order toward **parity with Orleans 10** (see [01](01-overview-and-goals.md)). A rolling
roadmap, not a single versioned release. The live status board is [`EPICS.md`](../EPICS.md); per-epic
design lives in the other `docs/` files and in [`docs/adr`](adr).

## Phases (shipped)

Each phase built on the previous and ended in something demonstrable, covered by sociable tests
(see [12](12-project-structure-and-tooling.md)) with a fake clock and deterministic placement.

1. **Single-silo core** — grains, identity, `Proxy` references, turn scheduler, catalog, activation
   lifecycle, the grain factory ([02](02-actor-model.md), [03](03-runtime-and-silo.md)).
2. **Messaging & multi-silo** — transport, message envelope, serializer, dispatcher, correlation; the
   DHT directory and placement; static membership ([04](04-messaging-and-serialization.md),
   [06](06-grain-directory-and-placement.md)).
3. **Kubernetes hosting** — EndpointSlice-watch membership, health endpoints, the `createSilo()`
   builder, graceful drain ([05](05-clustering-membership-k8s.md), [10](10-kubernetes-hosting.md)).
4. **Persistence** — `PersistentState` + etag concurrency; memory / Redis / Postgres
   ([07](07-persistence.md)).
5. **Timers & reminders** — in-memory timers; durable reminders with hash-range ownership and
   rebalancing; memory / Redis / Postgres tables ([08](08-timers-and-reminders.md)).
6. **Event streams** — pulling agents, ring-based queue ownership, durable cursors, at-least-once
   delivery, implicit subscriptions; memory / Redis Streams ([09](09-event-streams.md)).
7. **Cross-grain ACID transactions** — `TransactionalState<T>`, wait-die locking, an optimistic
   two-phase commit (TM elected from writers) with recovery ([ADR 0008](adr/0008-cross-grain-transactions.md)).

Authoring is **functional by default** (`defineGrain` + hooks, `defineReducerGrain`;
ADRs [0009](adr/0009-functional-grains.md)/[0010](adr/0010-message-dispatch-reducer-grains.md)) over a
retained class substrate. Also shipped: grain migration, grain-interface versioning
([ADR 0014](adr/0014-grain-interface-versioning.md)), implicit stream subscriptions, versioned
directory range handoff, placement filters, broadcast channels ([ADR 0015](adr/0015-broadcast-channels.md)),
grain call filters ([ADR 0012](adr/0012-grain-call-filters.md)), observability
([ADR 0013](adr/0013-observability.md)), durable journaling
([ADR 0019](adr/0019-durable-journaling.md)), the adaptive activation rebalancer
([ADR 0016](adr/0016-activation-rebalancer.md)), durable jobs ([ADR 0018](adr/0018-durable-jobs.md)),
and the external client with gateway discovery + failover.

## Remaining for parity

Orleans-10 parity is complete — all of the above ship.

## Deferred (not parity gaps)

Additional databases and stream backings behind the existing interfaces. Redis is the default;
Postgres grain storage and reminder table also ship.

## Beyond parity

**Browser state replication and browser-hosted grains** ([ADR 0017](adr/0017-browser-state-replication.md))
— replicate grain state to the browser as a live read-view, and eventually run permitted grains
client-side under a **server-enforced** trust model (the browser is untrusted, so "permitted to run
there" is a trust classification, not placement). v1 is a server-authoritative read-only read-view;
writable/optimistic/CRDT client state and browser-hosted grains are deferred to follow-up ADRs.
