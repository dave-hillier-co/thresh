# ADR 0004 — Kubernetes for membership and failure detection

- Status: Accepted
- Context doc: [05 — Clustering and membership on Kubernetes](../05-clustering-membership-k8s.md)

## Context

A cluster needs to know which silos are alive, discover their addresses, and detect failures.
Orleans implements this itself: silos write to a pluggable membership table, gossip status changes,
and probe one another in an expander-graph topology to detect failures, with many backing stores
(Azure Table, ADO.NET, Consul, ZooKeeper, DynamoDB, …). Orleans also ships a Kubernetes integration
(`Orleans.Hosting.Kubernetes`) where a cluster agent reconciles its membership against pod state.

We are committed to hosting on Kubernetes. Options considered:

1. **Delegate membership and failure detection to Kubernetes**: derive the live silo set from ready
   Pod endpoints (EndpointSlice watch) and use liveness/readiness probes as the failure detector.
2. **Port Orleans' own membership** (table + gossip + probe graph) and run it on top of Kubernetes.
3. **A hybrid**: Kubernetes for discovery, a custom probe protocol for failure detection.

## Decision

**Delegate membership and failure detection to Kubernetes** (option 1).

The silo watches the headless Service's EndpointSlices via the Kubernetes API (RBAC-scoped to
read/watch pods and endpoints in its namespace) to derive a versioned `MembershipSnapshot`. Failure
detection is liveness/readiness probes plus endpoint removal. Silo identity derives from pod
name + UID, removing the need for generation counters.

## Rationale

- **Avoid reimplementing a hard, already-solved problem.** Kubernetes provides stable identity,
  continuous health checking, discovery and reconciliation out of the box. Reproducing a correct
  gossip/probe failure detector is substantial, subtle work for no advantage on this platform.
- **Single source of truth.** Every silo reads the same EndpointSlices from the API server, so views
  converge without a gossip protocol; the watch stream is the push mechanism.
- **Operational familiarity.** Platform teams already reason about probes, endpoints, StatefulSets
  and rollouts. There is no separate membership store to provision, secure and monitor.
- **Smaller runtime.** Dropping the membership table, gossip and probe subsystems removes a large
  amount of code and configuration surface.
- **Precedent.** Orleans' own Kubernetes hosting moves in this direction by reconciling membership
  against pod state and killing orphaned silos.

## Consequences

- **The runtime is coupled to Kubernetes for production clustering.** Mitigated by a
  `MembershipService` interface with a `useStaticMembership` provider for local development and
  tests ([10](../10-kubernetes-hosting.md), [12](../12-project-structure-and-tooling.md)), so the
  core does not import Kubernetes types.
- **We inherit Kubernetes' eventual consistency.** EndpointSlices can briefly disagree across silos.
  Safety is preserved by the directory's CAS registration and `podUid` checks
  ([ADR 0003](0003-in-silo-dht-directory.md)); the worst case is a brief, bounded duplicate
  activation, not data loss.
- **Failure-detection timing is governed by probe configuration**, not a tunable in-process detector.
  Probe periods/thresholds become the knobs ([10](../10-kubernetes-hosting.md)).
- **RBAC is required.** Each silo needs a ServiceAccount with namespace-scoped watch on
  pods/endpoints/endpointslices.
- **Non-Kubernetes hosting is out of scope** for production. Acceptable per project goals; a
  different `MembershipService` implementation could be added later if needed.
