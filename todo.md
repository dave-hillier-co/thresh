# todo

Outstanding work only. See [`EPICS.md`](EPICS.md) for the status board, [`docs/`](docs/) for the
design, and [`docs/adr`](docs/adr) for the decisions. Test-first, vertical slices (see
[`CLAUDE.md`](CLAUDE.md)).

## Shipped

The Orleans-10 parity surface is in place and tested: the core actor model, messaging and multi-silo
routing, the DHT directory with versioned range handoff, placement (strategies + metadata filters +
version-aware), Kubernetes hosting, persistence and reminders (memory / Redis / Postgres), event
streams (memory / Redis) with implicit subscriptions, cross-grain ACID transactions, grain call
filters, observability, grain migration, grain-interface versioning, broadcast channels, the external
client (gateway discovery + failover), durable journaling (`DurableGrain`,
[ADR 0019](docs/adr/0019-durable-journaling.md)), and reducer / functional-first authoring. Worked
examples double as acceptance tests.

## Remaining

Nothing outstanding for Orleans-10 parity: the **activation rebalancer**
([ADR 0016](docs/adr/0016-activation-rebalancer.md)) and **durable jobs**
([ADR 0018](docs/adr/0018-durable-jobs.md)) both ship — see [`EPICS.md`](EPICS.md) for the board.

## Beyond parity

- [ ] **Browser state replication & browser-hosted grains** — design settled in
      [ADR 0017](docs/adr/0017-browser-state-replication.md) (read-only live read-views first);
      implementation pending.

## Deferred

- [ ] Additional stream backings behind the existing interfaces (Redis is the default). Postgres
      grain-storage and reminder providers already ship.
