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
- [0007 — Functional grains (factory closures instead of classes)](0007-functional-grains.md)
- [0008 — Message-dispatch reducer grains (no per-grain interface, no codegen)](0008-message-dispatch-reducer-grains.md)
- [0009 — Message dispatch as the substrate (typed interfaces are a compile-time view)](0009-message-dispatch-substrate.md)
