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
