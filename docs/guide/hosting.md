# Hosting and Kubernetes

`createSilo(config)` returns a fluent `SiloBuilder`. Configure membership, transport, required
providers, and grain registrations, then call `build()`, `start()`, and `stop()`. All silos in a
deployment share `clusterId` and stable logical `serviceId`.

## Production checklist

- Use WebSocket transport and Kubernetes membership.
- Advertise the pod IP/port and grant the service account EndpointSlice access.
- Replace memory providers with Redis, Postgres, or Kafka for durable data.
- Keep registrations, protocol versions, and provider names compatible across silos.
- Enable health endpoints and startup/readiness/liveness probes.
- Enable tracing, metrics, structured logging, and appropriate call filters.
- Handle `SIGTERM`, drain, and await graceful shutdown during rolling updates.
- Tune load shedding, turn queues, collection, timeouts, and resource limits.

The `examples/k8s-silo/deploy` directory shows the headless service, StatefulSet, RBAC, and Redis.
Its opt-in end-to-end test covers routing, pod loss, state survival, and rolling updates.

Builder groups include membership (`useStaticMembership`, `useKubernetesMembership`), transport
(`useInProcessTransport`, `useWebSocketTransport`), operations (`useHealthEndpoints`, `useTracing`,
`useMetrics`, `useLogging`), and memory/Redis/Postgres/Kafka providers.

## A pod's life in the cluster

Under `useKubernetesMembership` every silo reads its own status, and every peer's, from the
headless service's EndpointSlices, so silos that have seen the same slices hold the same view:

| Endpoint                       | Status     | Owns ranges and hosts new grains | A member |
| ------------------------------ | ---------- | -------------------------------- | -------- |
| ready                          | `active`   | yes                              | yes      |
| not ready, terminating         | `draining` | no                               | yes      |
| not ready, not terminating     | `joining`  | only while no member is active   | yes      |
| not listed                     | absent     | no                               | no       |

A **joining** pod (started, not yet marked ready by its readiness probe) owns no hash range and
activates no grain while any peer is active. It can already call grains — a startup task, a grain it
is asked for — and they are placed on, and answered by, the active silos. Its readiness probe passes
once its watch lists it; it does not wait to own anything. Once Kubernetes marks it ready it becomes
active, takes its ranges over, and new grains may be placed on it. A cold cluster, where no pod is
ready yet, forms its ring from the joining pods so that startup tasks can run; the first pod to
become ready takes the ring over and the others hand it their ranges.

A **draining** pod (being deleted) keeps serving the grains it already hosts until it stops, but
activates nothing new: a call to a new grain made through it is placed on an active silo.

A pod that Kubernetes **takes out of the Service while it is still running** (its endpoint removed
from the slices after it had been listed) fences itself: its peers have already dropped its
directory entries, so it refuses every call, abandons its activations without running their
deactivation hooks, stops its reminders, stream agents and durable jobs, and fails its liveness
probe, so the kubelet restarts it. Before its endpoint is first listed a pod is simply not yet a
member and is not fenced.

`useStaticMembership` lists every silo as active for good, with no liveness: a silo that exits stays
in the view and calls routed to it hang. A view of the silo alone is exact, and is what single-silo
development uses; a static view of several silos is refused at build unless
`useStaticMembership(silos, metadata, { allowWithoutLiveness: true })` accepts that. Clustered
deployments use `useKubernetesMembership`, and in-memory stores on a clustered silo follow the rule in
[State](state.md#choosing-where-state-lives).
