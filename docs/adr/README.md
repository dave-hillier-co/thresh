# Architecture decision records

Each ADR captures one significant decision, its context, the rationale, and its consequences.

- [0001 — Runtime ES `Proxy` grain references](0001-runtime-proxy-grain-references.md)
- [0002 — WebSocket/HTTP transport](0002-websocket-transport.md)
- [0003 — In-silo distributed grain directory (DHT)](0003-in-silo-dht-directory.md)
- [0004 — Kubernetes for membership and failure detection](0004-kubernetes-for-membership.md)
- [0005 — Redis as the default for persistence, reminders and streams](0005-redis-default-providers.md)
- [0006 — Reducer grains (event-routed, immutable state)](0006-reducer-grains.md)
- [0007 — Stream pulling agents and ring-based queue ownership](0007-stream-pulling-agents.md)
- [0008 — Cross-grain ACID transactions](0008-cross-grain-transactions.md)
- [0009 — Functional grains (factory closures instead of classes)](0009-functional-grains.md)
- [0010 — Message-dispatch reducer grains (no per-grain interface, no codegen)](0010-message-dispatch-reducer-grains.md)
- [0011 — Message dispatch as the substrate (typed interfaces are a compile-time view)](0011-message-dispatch-substrate.md)
- [0012 — Grain call filters](0012-grain-call-filters.md)
- [0013 — Observability (request context + OpenTelemetry tracing)](0013-observability.md)
- [0014 — Grain-interface versioning (version-aware placement)](0014-grain-interface-versioning.md)
- [0015 — Broadcast channels (lightweight in-cluster pub/sub)](0015-broadcast-channels.md)
- [0016 — Activation rebalancer (adaptive, entropy-minimizing)](0016-activation-rebalancer.md)
- [0017 — Browser state replication and browser-hosted grains](0017-browser-state-replication.md)
- [0018 — Durable jobs (sharded, durable, at-least-once scheduled execution)](0018-durable-jobs.md)
- [0019 — Durable journaling (`DurableGrain`)](0019-durable-journaling.md)
