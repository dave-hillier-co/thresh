# 01 — Overview and goals

## Why this project exists

Microsoft Orleans proved that the **virtual actor model** is an excellent way to build stateful,
scalable distributed systems without hand-writing distributed-systems code. A *grain* is virtual: it
always exists conceptually, is addressed by a stable identity, and is activated / deactivated / placed
/ located by the runtime on demand. Developers write what looks like ordinary single-threaded code, and
the runtime makes it scale and survive failures.

Orleans is .NET; this project brings the same model to **TypeScript/Node.js**, and makes a deliberate
hosting bet: **run on Kubernetes and delegate cluster mechanics to it**. Where Orleans ships a
gossip-based, probe-graph membership protocol, Kubernetes already provides stable identities, health
checking, discovery, scaling and rolling updates — so the runtime can be smaller and the operational
model more familiar.

## Goals

- **Faithful programming model** — grains, references, single-threaded turn-based execution,
  reentrancy, on-demand activation, managed lifecycle, persistence, timers, reminders, streams, and
  transactions map cleanly onto Orleans.
- **Idiomatic TypeScript** — typed `interface` + runtime `Proxy` references; grains authored as factory
  closures with hooks (the class + decorator form retained as the substrate / interop); `Promise`-based
  async; no build-time code generation.
- **Kubernetes-native operations** — membership, failure detection and discovery from Kubernetes; a
  `StatefulSet` behind a headless `Service` with liveness/readiness probes as the failure detector.
- **Pluggable durable backends** — persistence, reminders and streams are interfaces; **Redis is the
  default** for all three, with Postgres an alternative for persistence and reminders, and in-memory
  providers for dev/tests.
- **Operable and observable** — structured logging, OpenTelemetry traces/metrics, request-context
  propagation.

## Scope: Orleans 10 parity

The target is **feature parity with Orleans 10** for the actor model, persistence, timers and
reminders, streams, transactions, and Kubernetes hosting — "done" defined by what Orleans 10 offers,
not an internal version. [`EPICS.md`](../EPICS.md) tracks shipped vs. remaining.

## What Kubernetes replaces

| Orleans mechanism | Replaced by |
| --- | --- |
| Membership table + gossip | Kubernetes API watch on Pod endpoints |
| Probe-graph failure detection | Liveness/readiness probes + endpoint removal |
| Silo generation counters | Pod name + UID |
| Gateway list provider (for clients) | Kubernetes `Service` / DNS |
| Cluster discovery providers (Consul, ZooKeeper, …) | The Kubernetes control plane |

What Kubernetes does **not** replace — and we therefore implement — is the **grain directory** (which
silo hosts an activation) and **placement** (which silo activates a new grain), application-level
concerns built on the membership view ([06](06-grain-directory-and-placement.md)).

## How this differs from Orleans

The default is **faithfulness**; departures fall into exactly three sanctioned categories:

1. **TypeScript idioms** — runtime `Proxy` references and runtime-registered serialization instead of
   compile-time codegen (no build step; [ADR 0001](adr/0001-runtime-proxy-grain-references.md),
   [ADR 0011](adr/0011-message-dispatch-substrate.md)); WebSocket/HTTP transport
   ([ADR 0002](adr/0002-websocket-transport.md)); single-threaded execution enforced by a
   per-activation turn queue (same *guarantee* as Orleans, different mechanism; [02](02-actor-model.md)).
2. **Kubernetes-native hosting** — membership/failure-detection/discovery delegated to the orchestrator
   ([ADR 0004](adr/0004-kubernetes-for-membership.md)).
3. **Functional / reducer authoring** — grains as factory closures with hooks (`defineGrain` +
   `usePersistentState`/`useReducerState`) and a message-dispatch reducer grain (`defineReducerGrain`),
   layered over the Orleans-faithful class substrate ([ADR 0009](adr/0009-functional-grains.md),
   [ADR 0010](adr/0010-message-dispatch-reducer-grains.md)). All are the *same* virtual actor with
   identical guarantees.

Everything not in those categories is intentionally the same as Orleans — notably the grain directory,
an in-silo DHT over a consistent-hash ring ([ADR 0003](adr/0003-in-silo-dht-directory.md)).

## Glossary

- **Grain** — a virtual actor: stable identity + behaviour + optional state.
- **GrainId** — grain type + key (string/integer/guid); globally unique and stable.
- **Grain reference** — a typed, serializable proxy to call a grain (does not imply it is active).
- **Activation** — a concrete in-memory instance on a silo; at most one per grain (except stateless
  workers).
- **Silo** — the runtime host process, one per pod.
- **Cluster / membership** — the cooperating silos, derived from Kubernetes.
- **Grain directory** — the distributed map from `GrainId` to its hosting silo + activation.
- **Placement** — the policy choosing which silo activates a new grain.
- **Turn** — one uninterrupted unit of message processing on an activation (turns never overlap unless
  a method opts into reentrancy).
- **Reminder / Timer** — a durable persisted schedule / a non-durable in-memory one.
- **Stream** — a managed named channel of events with durable subscriptions and cursors.

The design is grounded in the Orleans source at `~/repos/orleans/src`; each deep-dive doc cites the
files its design derives from.
