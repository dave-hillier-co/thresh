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
activates nothing new: a call to a new grain made through it is placed on an active silo. The same
holds for stateless workers on a joining or draining pod: the workers it already has keep serving,
its pool does not grow, and a call it cannot serve from them runs on an active silo.

A pod that Kubernetes **takes out of the Service while it is still running** (its endpoint removed
from the slices after it had been listed) fences itself: its peers have already dropped its
directory entries, so it refuses every call, abandons its activations without running their
deactivation hooks, stops its reminders, stream agents and durable jobs, and fails its liveness
probe, so the kubelet restarts it. Before its endpoint is first listed a pod is simply not yet a
member and is not fenced.

## Shutting down

`SiloHost.stop()` (what a `SIGTERM` handler awaits) drains the silo in this order:

1. **Readiness flips** to not ready, before anything else stops. The ownership work stops with it:
   no reminder range, stream queue or durable-job shard is adopted from the moment the stop begins.
2. **The grace period** (`gracefulShutdownMs`, 5 s by default under Kubernetes membership, 0 under
   static membership) is waited out. It is the time Kubernetes' own proxies and the EndpointSlice
   controller need to stop routing to the pod, which the silo cannot observe.
3. **The handover.** The silo keeps following the EndpointSlice watch through its stop, so it sees
   its own endpoint turn terminating, as its peers do, and gives its share of the directory to the
   silos that remain. If it holds records the new owners have not yet collected, it waits for them,
   for at most **half of what is left of the stop budget after the grace period**. A silo that has
   not seen itself leave (its watch lags, or its endpoint was not marked terminating), that owned
   nothing, or that has nobody left to hand over to waits for nothing; its share is rebuilt from
   the survivors once it has gone. It goes on serving the grains it hosts throughout.
4. **Deactivation.** Every activation is deactivated (`onDeactivate` runs) with whatever is left of
   the budget, so deactivation always keeps at least half of what remained after the grace period.
   Each unregisters at the grain's new owner.
5. **The directory partition stops, the listener and connections close**, and the subsystems
   below the node (storage clients, the membership watch, the health listener) are torn down.

`stopBudgetMs` (30 s by default) bounds steps 2 to 4 together. Set it, with `gracefulShutdownMs`,
to fit inside the pod's `terminationGracePeriodSeconds`: a silo still deactivating when the kubelet
sends `SIGKILL` loses whatever its remaining `onDeactivate` hooks would have written. A pod whose
endpoint is removed from the slices before it has finished (step 3 or 4) is fenced, as a running pod
would be: its peers have already dropped its records, so the activations still up are abandoned
without their hooks, and the stop completes.

## When a pod is lost

Every silo owns a share of the grain directory: the record of which pod runs each grain. When
membership changes, the silos whose share grows lock the new part until they hold its records, so a
grain is never started a second time while its first activation may still be running. They take the
records from the silo that held them, or, when that silo is gone, rebuild them by asking every
listed pod which grains it runs.

- **A pod that stops gracefully** hands its records on while it is still serving, and deactivates
  only once they have been collected (see [Shutting down](#shutting-down)); nothing waits for its
  drain.
- **A pod that crashes** refuses connections once its process is gone, which counts as "I run
  nothing". As soon as its endpoint turns not ready, its share is rebuilt from the survivors, and its
  grains are started afresh on the next call.
- **A pod that is unreachable but still listed** — its node is partitioned or wedged, so dials to it
  time out — cannot answer, and the share being rebuilt stays locked until Kubernetes removes its
  endpoint. Calls to grains in that share that are not already in a caller's location cache wait, or
  time out with a rejection the caller may retry. On a node failure that is the node controller's
  eviction delay (the pod's `node.kubernetes.io/unreachable` toleration, five minutes by default):
  shorten it on silo pods to shorten the wait. A pod that cannot reach a listed peer when it starts
  does not become ready until it can, or until the peer is removed.

If availability matters more than the guarantee, `directoryRecoveryPatience` (unset by default)
bounds the wait: once it elapses the share unlocks with what the reachable pods reported, and a
grain the unreachable pod was running may be started a second time. Each such unlock is logged at
error level ("directory recovery patience elapsed") and counted.

```ts
createSilo({ clusterId, local, directoryRecoveryPatience: { seconds: 30 } });
```

The directory reports through the metrics seam: `thresh.directory.range_lock.wait` (how long
requests waited on a locked share), `thresh.directory.snapshot.transfers`,
`thresh.directory.recoveries.started` and `.completed`, `thresh.directory.recovery.retries` (a pod
asked again because it did not answer), and `thresh.directory.recovery.patience_expired`. A rising
retry count with recoveries that do not complete names a pod that is listed but unreachable; each
retry is also logged as a warning naming the pod.

`useStaticMembership` lists every silo as active for good, with no liveness: a silo that exits stays
in the view and calls routed to it hang. A view of the silo alone is exact, and is what single-silo
development uses; a static view of several silos is refused at build unless
`useStaticMembership(silos, metadata, { allowWithoutLiveness: true })` accepts that. Clustered
deployments use `useKubernetesMembership`, and in-memory stores on a clustered silo follow the rule in
[State](state.md#choosing-where-state-lives).
