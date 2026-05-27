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
client (gateway discovery + failover), and reducer / functional-first authoring. Worked examples
double as acceptance tests.

## Remaining

- [ ] **Activation rebalancer — slice 2b** ([ADR 0016](docs/adr/0016-activation-rebalancer.md)). The
      model (slice 1) and the distributed mechanism — load gathering, `migrateRandomActivations`,
      `runRebalanceCycle` (slice 2a) — ship. Remaining: an elected singleton worker driving the cycle on
      a timer (sessions/cycles, due-time/backoff), the `useActivationRebalancing(options?)` builder
      surface, a `RebalancingReport` + suspend/resume, and a convergence e2e over a running cluster.
- [ ] **Durable jobs** — implement [ADR 0018](docs/adr/0018-durable-jobs.md) (`@tsva/durable-jobs`): a
      sharded, durable, at-least-once scheduled-execution engine. Design only so far.
- [ ] **Durable journaling (`DurableGrain`)** — Orleans 10 `Orleans.Journaling`; needs an ADR
      (overlaps the reducer/persistent-state model) then implementation.

## Beyond parity

- [ ] **Browser state replication & browser-hosted grains** — design settled in
      [ADR 0017](docs/adr/0017-browser-state-replication.md) (read-only live read-views first);
      implementation pending.

## Deferred

- [ ] Additional stream backings behind the existing interfaces (Redis is the default). Postgres
      grain-storage and reminder providers already ship.
