# 05 — Clustering and membership on Kubernetes

A cluster is the set of silos cooperating to host one application's grains. The hard parts of
clustering — who is alive, how to find each other, how to detect failure — are delegated to Kubernetes.

> Orleans references: `Orleans.Runtime/MembershipService/*`,
> `Orleans.Core/SystemTargetInterfaces/IMembershipTable.cs`, `Orleans.Core/Runtime/SiloStatus.cs`,
> `Orleans.Hosting.Kubernetes/KubernetesClusterAgent.cs`.

## What we delegate, and why

Orleans runs its own membership (a membership table, status gossip, and a probe-graph failure
detector) because it must run anywhere. On Kubernetes the orchestrator already provides stable
identity (`StatefulSet` ordinals + DNS), continuous health checking (probes), discovery (a headless
`Service`'s ready endpoints), and reconciliation (failed pods replaced). So we let Kubernetes be the
membership authority and read the live silo set from it — the same direction Orleans' own
`Orleans.Hosting.Kubernetes` package takes. See [ADR 0004](adr/0004-kubernetes-for-membership.md).

## Silo identity

```ts
interface SiloAddress {
  podName: string;   // stable StatefulSet ordinal, e.g. "silo-2" — used by the directory ring
  podUid: string;    // unique per incarnation; changes when the pod is recreated
  endpoint: string;  // host:port via the headless Service DNS
}
```

`podName` is stable across restarts; `podUid` distinguishes a fresh incarnation, so stale directory
entries from a dead pod are recognised and discarded (Orleans uses `IP:port:generation` for the same).

## The membership service

```ts
interface MembershipService {
  current(): MembershipSnapshot;                 // live silo set + version
  updates(): AsyncIterable<MembershipSnapshot>;  // pushed on every change
  localSilo(): SiloAddress;
}
interface MembershipSnapshot { version: number; silos: ReadonlyArray<SiloMember>; }
interface SiloMember { address: SiloAddress; status: "joining" | "active" | "draining" | "dead"; metadata?: Record<string, string>; }
```

The four statuses are the subset of Orleans' `SiloStatus` a Kubernetes view can distinguish (`joining`
/ `active` / `draining` ↔ `ShuttingDown`+`Stopping` / `dead`); the whole `IMembershipTable` state
machine is replaced by reading these transitions off the API-server watch. The snapshot's `version` is
the **view number** that the directory and placement key off (Orleans' `MembershipVersion`); `metadata`
carries silo-advertised labels for metadata-aware placement ([06](06-grain-directory-and-placement.md)).

## How membership is derived

The silo authenticates with its pod `ServiceAccount` (RBAC scoped to *watch* in its namespace) and
**watches the EndpointSlices** of the headless Service selecting the silo `StatefulSet`. Ready
endpoints become `active` silos (pod name + UID from each endpoint's `targetRef`); disappearing
endpoints are marked `dead`; each reconciliation publishes a new versioned snapshot. Because every silo
watches the same source, views converge without gossip.

The implementation models this as a `WatchedEndpoints` source (aggregating added/modified/deleted watch
events) feeding a `KubernetesMembership` service; `createKubernetesClientSource` binds it to the real
`@kubernetes/client-node` watch (re-listing/re-watching when a watch closes). Parsing, aggregation and
reconciliation are unit-tested against fixtures; [`examples/k8s-silo`](../examples/k8s-silo) exercises
the whole path on a real cluster ([10](10-kubernetes-hosting.md)).

A silo always includes **itself** in its own view, even before its endpoint shows ready — correct (it
is a member of its own cluster) and necessary to bootstrap, since readiness gates on membership being
healthy, so a first/only pod would otherwise deadlock waiting to see a peer.

## Failure detection, split-brain, join/leave

A silo is failed when **Kubernetes removes it from the ready endpoint set** (liveness failure +
restart, readiness failure, or deletion/eviction). There is no Orleans-style probe graph. When a silo
leaves the view, every peer drops cached directory entries pointing at it, removes it as a placement
candidate, and rebuilds the affected directory ranges ([06](06-grain-directory-and-placement.md)).

EndpointSlices are eventually consistent, so two silos may briefly disagree about a third. The
directory's compare-and-set registration and `podUid` checks make this safe: duplicate activations
converge to one winner, and messages to a dead `podUid` are rejected so the caller re-resolves — the
cost of a brief split-brain is one redundant turn, not corruption (durability is the persistence
layer's job, [07](07-persistence.md)).

- **Join** — a new pod opens its transport, watches membership, marks itself `joining` then `active`;
  peers learn of it through their own watch.
- **Leave (graceful)** — on `SIGTERM` it marks itself `draining`, finishes in-flight turns, deactivates
  grains (flushing state), unregisters directory entries, and exits; readiness flips first so
  Kubernetes pulls it from endpoints ([03](03-runtime-and-silo.md)).
- **Leave (crash)** — no cleanup runs; peers detect endpoint removal and rebuild affected ranges.
