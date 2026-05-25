# 01 — Overview and goals

## Why this project exists

Microsoft Orleans proved that the **virtual actor model** is an excellent way to build stateful,
scalable distributed systems without writing distributed-systems code by hand. Its core insight is
that an actor (a *grain*) is *virtual*: it always exists conceptually, is addressed by a stable
identity, and is activated/deactivated/placed/located by the runtime on demand. Developers write
what looks like ordinary single-threaded object-oriented code, and the runtime makes it scale and
survive failures.

Orleans is .NET. This project brings the same programming model to **TypeScript/Node.js**, for
teams whose services and tooling live in the JavaScript ecosystem.

It also makes a deliberate hosting bet: **run on Kubernetes and delegate cluster mechanics to it**.
Orleans ships a sophisticated membership protocol (a gossip-based, probe-graph failure detector
backed by a pluggable membership table). On Kubernetes, the orchestrator already provides stable
identities, health checking, service discovery, scaling and rolling updates. We lean on those
primitives so the runtime can be smaller and the operational model more familiar to platform teams.

## Goals

- **Faithful programming model.** Grains, grain references, single-threaded turn-based execution,
  reentrancy, on-demand activation, managed lifecycle, persistence, timers, reminders and streams —
  the concepts a developer touches should map cleanly onto Orleans.
- **Idiomatic TypeScript.** Strongly-typed grain interfaces via `interface` + runtime `Proxy`
  references; decorators for declaration; `Promise`-based async throughout. No build-time code
  generation step.
- **Kubernetes-native operations.** Membership, failure detection and discovery come from
  Kubernetes. A `StatefulSet` of silos behind a headless `Service`; liveness/readiness probes as the
  failure detector.
- **Pluggable durable backends with sensible defaults.** Persistence, reminders and stream backing
  are interfaces. **Redis is the default** for all three; Postgres is a documented alternative for
  persistence and reminders; in-memory providers exist for development and tests.
- **Operable and observable.** Structured logging, OpenTelemetry traces/metrics, and request-context
  propagation across grain calls.

## Scope: Orleans parity

The target is **feature parity with current Orleans** for the actor model, persistence, timers and
reminders, streams, transactions, and Kubernetes hosting. "Done" is defined externally — by what
Orleans offers — rather than by an internal version label. The [roadmap](13-roadmap-and-phases.md)
tracks what is shipped versus what remains for parity.

## Non-goals

- **Multi-cluster / geo-distribution** (clustering across regions). Orleans itself **removed** its
  multi-cluster support in 3.0, so this is out of scope for parity, not merely deferred.
- **A bespoke high-performance binary wire protocol.** We use WebSocket framing and a pluggable
  serializer rather than reimplementing Orleans' networking stack.

Cross-grain ACID transactions ([ADR 0008](adr/0008-cross-grain-transactions.md)), grain-interface
versioning and implicit stream subscriptions are Orleans features and so are **parity work on the
roadmap**, not non-goals.

## What Kubernetes replaces

| Orleans mechanism | Replaced by | Notes |
| --- | --- | --- |
| Membership table + gossip | Kubernetes API watch on Pod endpoints | The live silo set is derived from the `StatefulSet`'s ready pods. |
| Probe-graph failure detection | Liveness/readiness probes + endpoint removal | A pod that fails its probe is removed from endpoints and treated as dead. |
| Silo generation counters | Pod name + UID | Pod identity is already globally unique per incarnation. |
| Gateway list provider (for clients) | Kubernetes `Service` / DNS | Clients connect through a service that load-balances across silos. |
| Cluster discovery providers (Consul, ZooKeeper, ADO.NET, …) | The Kubernetes control plane | One discovery mechanism instead of many. |

What Kubernetes does **not** replace, and we therefore still implement, is the **grain directory**
(which silo currently hosts a given activation) and **placement** (which silo should host a new
activation). Those are application-level concerns built *on top of* the membership view Kubernetes
gives us. See [06 — Grain directory and placement](06-grain-directory-and-placement.md).

## How this differs from Orleans

- **Grain references are runtime ES `Proxy` objects**, not compile-time generated classes. A
  decorator registers an interface's method table; the proxy turns calls into messages. See
  [ADR 0001](adr/0001-runtime-proxy-grain-references.md).
- **Transport is WebSocket/HTTP**, not a custom TCP protocol. See
  [ADR 0002](adr/0002-websocket-transport.md).
- **Membership is Kubernetes**, not a pluggable membership table with gossip. See
  [ADR 0004](adr/0004-kubernetes-for-membership.md).
- **Single-threaded execution is enforced by a per-activation turn queue**, since Node.js is
  cooperatively concurrent rather than truly single-threaded per object. The *guarantee* (one turn
  at a time per grain) is identical to Orleans; the mechanism differs. See
  [02 — The actor model](02-actor-model.md).

What is intentionally the *same*: the grain directory is an **in-silo distributed hash table** over
a consistent-hash ring, exactly as in Orleans, because grain-location entries are ephemeral and
rebuilt on membership change, so no external store is needed. See
[ADR 0003](adr/0003-in-silo-dht-directory.md).

## Glossary

- **Grain** — a virtual actor: stable identity + behaviour + optional state. The unit a developer
  writes.
- **Grain identity (`GrainId`)** — grain type + key (string, integer or guid). Globally unique and
  stable for the life of the application.
- **Grain reference** — a strongly-typed, serializable proxy used to call a grain. Obtained from
  the grain factory / client; does not imply the grain is active.
- **Activation** — a concrete in-memory instance of a grain on a particular silo. A grain has at
  most one activation at a time (except stateless workers).
- **Silo** — the runtime host process. One silo per Kubernetes pod. Hosts many activations.
- **Cluster** — the set of silos cooperating to host one application's grains.
- **Membership / membership view** — the current set of live silos, derived from Kubernetes.
- **Grain directory** — the distributed map from `GrainId` to the silo + activation hosting it.
- **Placement** — the policy that chooses which silo activates a new grain.
- **Turn** — a single, uninterrupted unit of message processing on an activation. Turns for one
  activation never overlap unless the method opts into reentrancy.
- **Reminder** — a durable, persisted timer that survives deactivation and pod restarts.
- **Timer** — a non-durable, in-memory timer tied to an activation's lifetime.
- **Stream** — a managed, named channel of events with durable subscriptions and cursors.

## Reference

The design is grounded in the Orleans source at `~/repos/orleans/src`. Each deep-dive document
cites the specific Orleans files its TypeScript design derives from, so implementers can compare
against the original.
