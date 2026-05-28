# Experiment: the Kubernetes shard layer — where the actor model meets K8s

> **Status: experiment.** This is a design exploration, not a committed direction. It argues for a
> thin **shard layer** as the seam between Kubernetes and the in-process activation layer, and shows
> what that one abstraction lets you delete. See
> [`experiment-reducer-snapshot.md`](experiment-reducer-snapshot.md) for the complementary
> authoring-layer experiment and [`deviations.md`](deviations.md) for what is true today (Kubernetes
> is already the membership authority).

## The granularity problem

Kubernetes primitives are **pod-granular and heavyweight**: an etcd object is ~KB, writes and watches
settle in 100ms–seconds, a pod is hundreds of MB and seconds to schedule. Actors are **object-granular
and cheap**: KB of state, sub-millisecond create/move, potentially 10^6 of them. That is ~6 orders of
magnitude. So the design question is never "use K8s or not" — it is **where to put the seam between
the K8s-managed coarse layer and the in-process fine layer.** You cannot push K8s objects down to the
activation layer. One actor per container is six orders of magnitude too coarse; even one shard per
pod is the same mistake one rung up unless shards are few.

## The granularity ladder

| Layer | Count | Owner | What lives here |
| --- | --- | --- | --- |
| Cluster | 1 | Namespace / StatefulSet | the deployment |
| **Pod / silo** | 10s–100s | **Kubernetes (native)** | identity, liveness, membership, scaling, rolling update, Leases |
| **Shard** | fixed `S` (e.g. 256–4096) | **the seam** — runtime logic, optional K8s record | ownership, handoff, and the directory / reminders / streams / jobs *for its keys* |
| Activation / grain | 10^3–10^6 | runtime only (never K8s) | activate-on-demand, turn scheduler, idle deactivation, snapshot |

K8s gives the pod layer for free (the runtime already takes it). The runtime *must* own the activation
layer — far too fine for etcd. **The shard is the only place the design actually lives:** the coarsest
unit still finer than a pod, and the unit of ownership, handoff, and rebalancing.

## The single knob: shard count `S`

How you pick `S` and map shards to pods positions the whole design:

- **`S` = pod count** → shards are implicit (= ordinal); no K8s objects; but a scale event remaps
  *everything* (migration storm).
- **`S` = grain count** → no sharding; back to a per-grain directory; etcd melts. This is the "one
  actor per K8s object" failure.
- **`S` = fixed, large, ≫ pods** → the sweet spot. Map shards to the live pod set with **rendezvous
  (highest-random-weight) hashing**: for each shard pick the pod maximizing `hash(shard, pod)`. Not
  `shard % N` — modulo reshuffles most shards when `N` changes, while rendezvous moves only ~`1/N`.
  Rendezvous needs no ring state: every pod computes the same assignment from the membership set it
  already watches. Leaderless, deterministic, agreement for free.

So "lean into K8s" means precisely: **K8s owns pods; a fixed set of `S` shards is rendezvous-hashed
onto the live pods; grains are computed onto shards (`hash(grainId) % S`); the pod owning a shard hosts
its activations.**

## What the one shard abstraction subsumes

The runtime today has three bespoke consistent-hash ownership schemes (reminder ranges, stream-queue
ownership, durable-job shards), plus an activation rebalancer, plus a DHT directory. All collapse onto
"owned per shard":

- **The directory deletes entirely** when placement is deterministic — you *compute* a grain's pod
  from the watched pod set, never look it up or register it. (Cost: no per-grain adaptive placement;
  a single hot grain cannot move without its whole shard. Mitigate with a small etcd/CRD *exception
  table* for the few pinned or hot cases — bounded, so etcd-safe.)
- **The activation rebalancer mostly evaporates** — the hash spreads load statistically; what remains
  is *shard* rebalancing on scale, which is just "recompute the rendezvous map when the pod set
  changes." No entropy-minimizing algorithm.
- **Reminders / streams / jobs** become "the work for the shards I own" — one ownership mechanism
  instead of three.

## The one piece of real coordination: safe handoff

When a shard moves pod A→B on a scale or failure event, A and B observe the membership change at
slightly different times (watch latency), so two pods could briefly host the same grain — and two
snapshot writers is a lost update. This is the single place a K8s primitive earns a per-shard object:

- **Per-shard `coordination.k8s.io/Lease`** (bounded count `S`, so etcd-safe): B must acquire the
  shard's Lease before hosting; A holds it until it has drained (deactivated its grains and flushed
  snapshots). The **etag on the snapshot is the backstop** — a stale write from A fails CAS. The same
  compare-and-swap primitive appears at the cluster layer (Lease / `resourceVersion`) and the grain
  layer (etag); see [`experiment-reducer-snapshot.md`](experiment-reducer-snapshot.md).
- Or **generation-fenced writes**: each shard assignment carries a generation, writes carry it, and
  stale-generation writes are rejected — the per-shard analogue of Orleans' silo generation.

Minimal-but-correct: **rendezvous-hashed shards over the pod set + a per-shard Lease for handoff + the
etag backstop.**

## The other "what layer": the implementation pattern

A distinct axis — *where the logic runs* relative to K8s:

1. **Leaderless library in the silo** (today's shape) — each pod watches the API server, computes the
   rendezvous map itself, and takes Leases for its shards. No extra component, leaderless. **The
   recommended baseline.**
2. **Central placement service (Dapr-style)** — a separate Deployment computes assignments and pushes
   them to sidecars. The closest shipping prior art for virtual actors on K8s, but it adds a central
   service and its own HA. Rendezvous hashing lets us be *lighter*: no central placement service.
3. **Controller + `Shard` CRD** — a controller reconciles shards→pods and shards become
   `kubectl get shards` objects. Worth it only to make shards externally observable or manageable; the
   "K8s-native and inspectable" upgrade over (1).
4. **Custom scheduler / extender** — wrong layer. The K8s scheduler places pods; it should never reason
   about grains.

## The storage sub-layer trap

Stable ordinals plus a per-ordinal PVC would make a shard's snapshots a node-local disk read instead of
a remote round-trip. But a PVC is pinned to an ordinal, and rendezvous rebalancing moves shards *across*
ordinals, stranding the data. Local PV only works if shard ≡ ordinal and ordinals never reshuffle, which
fights the rebalance we want. So shard mobility keeps the snapshot store external (Redis / Postgres /
etcd); the storage layer stays above the pod.

## Honest costs

- **The hot-shard problem.** Deterministic placement gives statistical load balancing, not adaptive —
  a single hot grain cannot be peeled off a hot pod without moving its whole shard. The exception
  table mitigates the few cases that matter.
- **A brief unavailability window** for shards in flight during a scale event (watch lag + drain),
  bounded by the handoff protocol.
- **The etcd ceiling** only bites if `S` is made huge or the exception table grows unbounded; keeping
  both bounded is what keeps the coordinator-backed design viable.
