# ADR 0003 — In-silo distributed grain directory (DHT)

- Status: Accepted
- Context doc: [06 — Grain directory and placement](../06-grain-directory-and-placement.md)

## Context

The runtime must answer "which silo currently hosts the activation for this `GrainId`?" for every
call that is not served from cache. Orleans implements this as a distributed hash table: a
consistent-hash ring partitioned across silos, each owning virtual nodes, with versioned handoff on
membership change (`DistributedGrainDirectory`, `GrainLocator`, `DhtGrainLocator`).

Because we are hosting on Kubernetes and already lean on it for membership
([ADR 0004](0004-kubernetes-for-membership.md)), it is tempting to also keep grain locations in an
external store. Options considered:

1. **In-silo distributed DHT** over a consistent-hash ring (the Orleans design), built on the
   Kubernetes-derived membership view.
2. **External store** (e.g. Redis) holding all `GrainId -> address` entries.
3. **Kubernetes-native** (etcd via CRDs/ConfigMaps) as the directory.

## Decision

Use the **in-silo distributed DHT** (option 1).

## Rationale

- **Grain-location entries are ephemeral.** An entry only means "X is activated on silo Y *right
  now*". If Y dies the entry is worthless and must be discarded; the grain reactivates elsewhere.
  Disposable, reconstructable data does not need a durable store.
- **Keeps the hot path off external dependencies.** Lookups are mostly cache hits; misses hit a
  peer silo's in-memory partition, not Redis or the API server. This avoids adding a per-call
  dependency (and its latency, failure modes and load) to the most frequent operation in the system.
- **The membership view already gives us a consistent ring.** Every silo computes the same ring from
  the same `MembershipSnapshot` version ([05](../05-clustering-membership-k8s.md)), so partition
  ownership needs no separate coordination service.
- **Faithful to Orleans**, so its proven properties (compare-and-set registration for at-most-one
  activation, cache invalidation hints, range rebalancing) carry over directly.

An external Redis directory would put Redis on the critical path of every cache miss and turn a
self-healing ephemeral structure into shared mutable state requiring careful invalidation on silo
death. A Kubernetes-native directory would stress etcd with high-churn, high-cardinality writes it
is not designed for. Both couple core routing to infrastructure unnecessarily.

Note the contrast with **persistence, reminders and streams**, which *are* durable and therefore
*do* use an external store (Redis by default) — see
[ADR 0005](0005-redis-default-providers.md). The directory is different precisely because its data
is not worth persisting.

## Consequences

- **We implement ring math, partitioning, registration CAS, caching and rebalancing.** This is the
  most algorithmically involved part of the runtime.
- **Membership-change rebalancing must be correct and version-linearised.** Mitigated by keying all
  directory state off the membership view version so a silo never mixes two ring topologies
  ([06](../06-grain-directory-and-placement.md)).
- **Range handoff is staged.** Phase 2 ([13](../13-roadmap-and-phases.md)) uses a simpler
  drop-and-lazily-rebuild on join (a few redundant reactivations) and adds Orleans-style versioned
  handoff later. This trades some efficiency for much less initial complexity.
- **Brief duplicate activations are possible during split-brain** but bounded and safe: CAS
  registration and `podUid` checks converge to one winner, and durability is the persistence layer's
  responsibility, not the directory's.
