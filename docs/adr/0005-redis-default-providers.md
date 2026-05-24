# ADR 0005 — Redis as the default for persistence, reminders and streams

- Status: Accepted
- Context docs: [07 — Persistence](../07-persistence.md),
  [08 — Timers and reminders](../08-timers-and-reminders.md),
  [09 — Event streams](../09-event-streams.md)

## Context

Three subsystems need a **durable** backing store (unlike the grain directory, which is ephemeral —
[ADR 0003](0003-in-silo-dht-directory.md)):

- **Persistence** — grain state that must survive deactivation and pod loss.
- **Reminders** — durable schedules that must fire even after restarts.
- **Streams** — event queues with durable subscriptions and cursors.

Orleans makes each of these a pluggable provider with many implementations (Azure, ADO.NET,
DynamoDB, Redis, in-memory). We want pluggability too, but also a clear default so the docs,
examples and hosting builder are concrete and a new deployment works without choosing a backend per
subsystem.

Options considered for the default:

1. **Redis** for all three.
2. **Postgres** for all three.
3. **In-memory** default with durable providers opt-in.

## Decision

**Redis is the default** for persistence, reminders and streams. In-memory providers remain for
development and tests. **Postgres** is a documented alternative for persistence and reminders. The
provider interfaces (`GrainStorage`, `ReminderTable`, `StreamProvider`) keep all of these
substitutable.

## Rationale

- **One dependency covers all three.** A single Redis fits key-value state, range-scannable reminder
  entries, and Redis Streams for event queues — so a deployment provisions one backing service, not
  three.
- **Operationally simple on Kubernetes.** Managed Redis is ubiquitous; an in-cluster Redis is easy
  for dev. Low latency suits the explicit `write()` persistence model and stream throughput.
- **Redis Streams fit the stream design directly.** Consumer groups provide per-queue acknowledgement
  and redelivery, which is exactly what the at-least-once pulling-agent architecture needs
  ([09](../09-event-streams.md)).
- **Etag concurrency is straightforward.** Compare-and-set via `WATCH`/`MULTI` or a small Lua script
  implements the optimistic concurrency persistence and reminders require
  ([07](../07-persistence.md)).
- **Defaults should be concrete.** Picking a single default keeps examples runnable and removes a
  decision from getting started, while the interfaces preserve choice.

In-memory as the default would mean the out-of-the-box experience is non-durable, which is a poor
default for a stateful actor runtime. Postgres is an excellent durable choice — and is offered for
persistence/reminders, especially where state must be queried/reported on relationally — but it does
not cover streaming as cleanly as Redis Streams, so making it the single default would still require
a second system for streams.

## Consequences

- **Redis is a production dependency by default.** Its availability/durability configuration (e.g.
  persistence mode, replication) becomes an operational concern documented for deployers.
- **Stream retention/rewind is bounded by Redis Stream trimming policy** ([09](../09-event-streams.md)).
- **Provider parity is a testing obligation.** A shared provider conformance test suite runs against
  in-memory, Redis and (where applicable) Postgres so alternatives behave identically
  ([12](../12-project-structure-and-tooling.md)).
- **Postgres stream support and other backends are future work**, added behind the same interfaces
  per the [roadmap](../13-roadmap-and-phases.md).
