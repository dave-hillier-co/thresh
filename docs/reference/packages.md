# Package map

Thresh uses subpath imports; there is intentionally no barrel export. Import the defining file, for
example `@thresh/core/define-grain`.

| Package | Responsibility |
|---|---|
| `@thresh/core` | contracts, identity, authoring, lifecycle, state interfaces, streams, reminders |
| `@thresh/hosting` | silo builder/lifecycle, providers, health endpoints |
| `@thresh/runtime` | activations, scheduling, placement, cluster node, grain services |
| `@thresh/client` | external clients, gateways, observers |
| `@thresh/messaging` | serializers and in-process/WebSocket transport |
| `@thresh/directory` | activation directory and location cache |
| `@thresh/persistence` | memory, Redis, and Postgres grain storage |
| `@thresh/transactions` | coordination and transactional storage |
| `@thresh/journaling` | journals, snapshots, durable collections |
| `@thresh/reminders` | reminder service and durable tables |
| `@thresh/streams` | stream providers, subscriptions, cursors |
| `@thresh/durable-jobs` | durable shard-backed jobs |
| `@thresh/clustering-k8s` | EndpointSlice watching and membership |
| `@thresh/observability` | OpenTelemetry and logging filters |
| `@thresh/testing` | multi-silo `TestCluster` |
| `@thresh/parity` | Orleans port/scorecard tests; not an app dependency |

The TypeScript source is the exact API reference. Start with `core/define-grain.ts`,
`core/grain-interface.ts`, `hosting/silo-builder.ts`, and `testing/test-cluster.ts`.
