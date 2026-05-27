# todo

Outstanding work only. See [`EPICS.md`](EPICS.md) for the status board and
[`docs/deviations.md`](docs/deviations.md) for how the design differs from Orleans. Test-first,
vertical slices (see [`CLAUDE.md`](CLAUDE.md)).

## Shipped

The Orleans-10 parity surface is in place and tested: the core actor model, messaging and multi-silo
routing, the DHT directory with versioned range handoff, placement (strategies + metadata filters +
version-aware), Kubernetes hosting, persistence and reminders (memory / Redis / Postgres), event
streams (memory / Redis) with implicit subscriptions, cross-grain ACID transactions, grain call
filters, observability, grain migration, grain-interface versioning, broadcast channels, the external
client (gateway discovery + failover), durable journaling (`DurableGrain`), durable jobs, the
activation rebalancer, and reducer / functional-first authoring. Worked examples double as acceptance
tests.

## Beyond parity

- [ ] **Browser state replication & browser-hosted grains** — read-only live read-views first;
      implementation pending.

## Deferred

- [ ] Additional stream backings behind the existing interfaces (Redis is the default). Postgres
      grain-storage and reminder providers already ship.
