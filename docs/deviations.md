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
- **A grain interface is a value, not an attribute.** `defineGrainInterface<T, K>(name)` (or the
  interface a `defineGrain` definition carries) is the thing callers pass to `getGrain`. Its **id is
  `stableHash32(name)`** — the name *is* the wire identity, so a rename is a wire break with no
  compile-time signal (see below).
- **A grain's key kind is a type argument, not a marker interface.** `"string"` (the default),
  `"integer"`, `"guid"`, `"integer-compound"` or `"guid-compound"`, stated once —
  `defineGrainInterface<ICounter, "integer">("ICounter")` — and it determines the type of the `key`
  argument at every `getGrain`. The legacy `GrainWith*Key` phantom markers still work and still imply
  the same kind, but they are deprecated; new code states the kind.
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
and interop surface. **Reducer** and **single-dispatch** grains (`defineReducerGrain`) add an
event-folding authoring shape on top. These change how a grain is *written*, not what it *is* — the
runtime, guarantees, and lifecycle are unchanged.

### Rules of hooks

`usePersistentState`, `useReducerState`, `useTransactionalState`, `useDurableState` /
`useDurableDictionary` / `useDurableList` / `useDurableQueue` / `useDurableSet`,
`useDurableJobHandler` and `useContext` take **no context argument**. They resolve the activation
being set up from an ambient slot that the runtime pushes around the factory call and pops in a
`finally`. That slot is live only for the *synchronous* body of the factory, so:

- call hooks at the **top level of the factory**, before any `await`;
- calling one outside a factory, after an `await`, from a method body or from `onActivate` throws,
  naming the hook;
- an `async` factory is rejected at activation with "factories must be synchronous" — the ambient
  slot has already been popped by the time the promise resolves.

The window is synchronous end to end (nothing awaits between the push and the pop), and the slot is a
stack, so a factory that activates another grain does not steal the inner one's facet registrations.

The factory's **`ctx` parameter stays**, and it is how a grain reaches the runtime *after* setup:
capture it in the closure and use `ctx.runtime`, `ctx.id` and `ctx.getGrain` inside method bodies and
lifecycle hooks. `useContext()` returns the same value, for helpers that would otherwise thread `ctx`
through — but only during setup, like any other hook.

### Definitions are interfaces

`defineGrain(name, factory, options?)` returns a `GrainDefinition<T, K>`, which *extends*
`GrainInterface<T, K>` and additionally carries `.grain`, the implementation constructor. So one value
is both the registration and the contract: `registerGrain(Thermostat)` and
`getGrain(Thermostat, key)`. The message surface `T` is **inferred from what the factory returns** —
string-keyed function members, promise-lifted, minus `onActivate`/`onDeactivate` and minus
symbol-keyed system hooks (`INCOMING_CALL_FILTER`, `STREAM_SUBSCRIPTION_OBSERVER`,
`BROADCAST_CHANNEL_OBSERVER`, `DURABLE_JOB_HANDLER`). Passing `T` explicitly
(`defineGrain<Ledger, "integer">(…)`) pins the contract instead of inferring it, so an incidental
helper on the returned object cannot widen the wire surface.

Fusion is a **trade-off, not a strict improvement**. Inferring the contract from the implementation
means every caller imports the implementation module, and transitively its storage, stream and job
dependencies. That is fine in-silo and within a package. Across a package or process boundary — an
external `@thresh/client` app that today imports a ten-line interface module, an interface with more
than one implementation, or a contract with no implementation at all — keep the interface
**separately declared** with `defineGrainInterface`, and register the pair:
`registerGrain(LedgerGrain, { interfaces: [Ledger] })`. `defineGrainInterface` is a first-class,
recommended API, not a fallback.

Registration resolves to one `{ ctor, interfaces }` pair everywhere (`Silo`, `ClusterNode`,
`SiloBuilder`, `ClientNode`, `TestCluster`). **An explicit `interfaces` list is used exactly as
given** — the definition's own interface is not appended to it. A definition contributes its own
interface only when no list is supplied. A bare constructor still requires the list, because it
carries no interface; one constructor under several interfaces, and one constructor under per-silo
interface *versions* for a rolling upgrade, remain expressed that way.

### Interface names are wire identity

The id is `stableHash32(name)`, so **renaming an interface repoints every deployed caller** and
nothing catches it at compile time. This is the migration hazard worth stating twice: a grain moving
from a separately declared `defineGrainInterface("example.IThermostat")` to a fused
`defineGrain("Thermostat")` changes its id unless it pins the old name with
`{ interfaceName: "example.IThermostat" }`. Any grain that has ever been deployed must pin it.

Two consequences of fusion in the process-wide interface registry:

- **`defineGrain` also registers an interface under the grain-type name.** A grain type called
  `Counter` now puts a `"Counter"` entry in the registry alongside whatever separately declared
  interface it answers to. More names in the registry means a slightly larger `stableHash32` collision
  surface; a genuine collision (two different names, same id) throws at definition time rather than
  silently taking over the other's calls.
- **Redefining a name accumulates rather than resets.** `defineGrainInterface` merges into any entry
  already registered under the same id: per-method `options` union (the later definition wins per
  method), and `extension` and `key` are inherited when the later definition omits them (a
  disagreeing `key` throws). `version` is *not* merged — one name at two versions is the
  rolling-upgrade path. Merging is what lets a hand-written interface module and a same-named fused
  definition coexist without the second silently dropping the first's `extension: true`, which a
  *receiving* silo reads back out of the registry.

## Additions beyond Orleans

A few capabilities layer on top of the faithful model without changing it: **durable journaling**
(a grain that journals each mutation and replays it on activation) and **durable jobs** (sharded,
at-least-once scheduled grain invocation). A further, **not-yet-built** direction is **browser state
replication** — a live read-view of grain state in the browser under a server-enforced trust model.
