# How this differs from Orleans

This is a faithful TypeScript port of Orleans' virtual-actor model, hosted on Kubernetes. **Almost
everything works as it does in Orleans** — the actor model, the grain directory and placement,
persistence, timers and reminders, streams, and cross-grain ACID transactions. For the mechanics of
any of those, read the Orleans source; we don't re-document it. What follows is the high-level
summary of the deliberate deviations. [`EPICS.md`](../EPICS.md) tracks what is shipped; the
[`README`](../README.md) has the intent and a quick example.

## TypeScript idioms

- **References are a runtime ES `Proxy`**, not compile-time generated code, and calls dispatch by
  **method name** — a typed interface is a compile-time view, with no generated method table. There
  is no build step.
- **Serialization is registered at runtime** (`@serializable`) rather than source-generated;
  MessagePack is the default wire format.
- **Transport is WebSocket over HTTP**, behind an abstraction.
- **Single-threaded turns** are enforced by a per-activation turn scheduler. The guarantee is
  identical to Orleans; the mechanism differs because `await` yields the Node event loop.
- The code is a **pnpm workspace of small `@thresh/*` packages** with no barrel files and standard
  TC39 decorators (no `reflect-metadata`), run straight from source.

## Kubernetes-native hosting

Kubernetes is the **membership authority**, replacing Orleans' membership table, status gossip, and
probe-graph failure detector. A silo watches the EndpointSlices of a headless `Service`: ready
endpoints are live silos, removed endpoints are dead. Silos run as a `StatefulSet` (stable ordinals
keep a restarted silo in the same ring position); liveness/readiness probes drive failure detection
and the graceful drain. Silo identity is `podName` + `podUid` rather than Orleans' `IP:port:generation`.

| Orleans mechanism | Replaced by |
| --- | --- |
| Membership table + gossip | Kubernetes API watch on Pod endpoints |
| Probe-graph failure detection | Liveness/readiness probes + endpoint removal |
| Silo generation counters | Pod name + UID |
| Gateway list provider (clients) | Kubernetes `Service` / DNS |
| Cluster discovery providers | The Kubernetes control plane |

## Functional / reducer authoring

Grains are authored as **factory closures** (`defineGrain` + hooks like `usePersistentState`) rather
than classes with attributes; the Orleans-style class form is retained underneath as the substrate
and interop surface. **Reducer** and **single-dispatch** grains add an event-folding authoring shape
on top. These change how a grain is *written*, not what it *is* — the runtime, guarantees, and
lifecycle are unchanged.

## Additions beyond Orleans

A few capabilities layer on top of the faithful model without changing it: **durable journaling**
(a grain that journals each mutation and replays it on activation) and **durable jobs** (sharded,
at-least-once scheduled grain invocation). A further, **not-yet-built** direction is **browser state
replication** — a live read-view of grain state in the browser under a server-enforced trust model.
