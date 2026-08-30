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

## Bounded CAS retry under custom-storage log consistency

A `JournaledGrain` that also implements `CustomStorageInterface` owns its own log persistence,
mirroring Orleans' `ICustomStorageInterface<TState, TDelta>`. One thing deliberately differs.

Orleans' `CustomStorageAdaptor.WriteAsync` is *stubborn*: when the compare-and-set is rejected it
re-reads storage and retries **forever**, because that retry runs on a background log-consistency
protocol loop and nothing is waiting on it. Thresh has no such loop — `confirmEvents()` is awaited
inside the grain turn — so an unbounded retry would hold the activation until the stuck-turn
watchdog fired, turning a storage conflict into a hang.

The adaptor therefore retries a bounded number of times (5 by default, configurable) and then
throws `InconsistentStateError`, carrying the expected and stored versions in the etag fields. The
events stay pending, so a later `confirmEvents()` retries them: the caller chooses whether to keep
waiting, rather than the framework deciding for it.

## Cancellation reaches inside an argument

Orleans scans only **top-level** arguments for a `GrainCancellationToken`, on both legs of a call
(`GrainReferenceRuntime.SetGrainCancellationTokensTarget` records the call's target on the token;
`CancellationSourcesExtension.RegisterCancellationTokens` swaps the wire token for the activation's
own). A token nested inside a request record therefore never records a target and is never
registered, so cancelling it does not reach the callee.

Thresh walks the argument graph instead, so a cancellation value nested inside a plain object, an
array, a `Map` value or a `Set` member is converted, has the call's target recorded on it, and is
unwrapped on the callee exactly as one in its own parameter slot. This matters more here than it
does in Orleans because Thresh's cancellation shape at the API surface is a plain `AbortSignal`,
which has no wire representation at all: left unconverted, a nested one reaches a cross-silo callee
as an inert object rather than as a live-but-uncancellable token.

Two bounds on that walk. It does **not** descend into class instances (or grain references), because
rebuilding one would hand a same-silo callee a plain object where its signature declares the class —
so a signal held by a class-typed record still does not cross a silo boundary. And a value graph
containing a cycle is left alone at the point it closes, since a cyclic argument is legal on a
same-silo call, which never serializes.

## A collection age shorter than the sweep interval is legal

Orleans' `GrainCollectionOptionsValidator` rejects, at host start, any `CollectionAge` — the
cluster default or a `ClassSpecificCollectionAge` entry — that is not strictly greater than
`CollectionQuantum`, the collector's sweep period. Thresh validates only that a configured age is
a finite number of seconds greater than zero, and accepts one shorter than
`collectionIntervalSeconds`.

The rule exists in Orleans because its collector buckets activations by a ticket derived from the
quantum, so an age below one quantum has no bucket to land in. Thresh's collector is a plain
periodic sweep that compares each activation's idle time against its own age limit, and a
sub-sweep age is therefore meaningful, not degenerate: the activation is collected on the first
sweep at or after its age elapses. Adopting Orleans' rule would reject a configuration that
behaves correctly here, and would break the short ages tests legitimately configure against the
default 60s sweep.

## An observer reference from a silo needs an in-process gateway

Orleans' `IGrainFactory.CreateObjectReference` works on any silo, because an Orleans client leg is
**duplex over its own outbound connection**: the gateway answers a client on the socket the client
dialled, so hosting an observer costs the client nothing but a registration.

Thresh's `ClientNode` is not duplex. `connect()` listens on its own endpoint, and a silo delivers a
call to a client-hosted object by **dialling that advertised endpoint** — over the in-process
network that is free, but over `WebSocketTransport` it is a second real listening port and a
reachable address, which `SiloConfig` does not supply. So the embedded client that backs
`GrainFactoryAccess.createObjectReference` for a startup task (Orleans' `IStartupTask` hook, which
upstream's `LifecycleObserverCreationTests` exercises with exactly this call) exists only on a silo
built with `useInProcessTransport(network)`.

The sharp edge is not the restriction, it is where it surfaced: `TestCluster` always configures an
in-process transport, so an observer push path could be green in every test and throw on the first
call in production. `SiloBuilder.requireObserverHosting()` is the declaration that closes that — a
silo that depends on the seam says so, and a transport that cannot back it is rejected at `build()`
rather than at first use. Declaring nothing keeps the old behaviour, because the common startup task
never touches the seam.

## Additions beyond Orleans

A few capabilities layer on top of the faithful model without changing it: **durable journaling**
(a grain that journals each mutation and replays it on activation) and **durable jobs** (sharded,
at-least-once scheduled grain invocation). A further, **not-yet-built** direction is **browser state
replication** — a live read-view of grain state in the browser under a server-enforced trust model.
