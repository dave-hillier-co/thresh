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
- **A grain call carries `undefined` back as `undefined`**, not as `null`, whether the callee is
  same-silo or cross-silo. C# has one "no value" (`null`); TypeScript has two, and a method declared
  `Promise<T | undefined>` — the shape every ported `Task<T?>` takes — must hand its caller the one
  its own signature promises, or a `=== undefined` guard downstream fails open on a `null`. Neither
  wire format can carry a bare `undefined` (MessagePack writes nil, `JSON.stringify` yields no
  string at all), so `encodeValue` tags a **top-level** `undefined` and `decodeValue` restores it.
  Only the top level: a `undefined` MEMBER of a returned object still travels as an omitted key.

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

## A cancellation crosses a silo boundary AS a cancellation

`GrainCallAbortedError` and `GrainTaskCanceledError` carry no enumerable own properties, so the
codec's generic object branch used to flatten them to `{}` and a cross-silo caller saw a bare
`GrainCallError`: `isCancellationError` was false, and a deliberate abort was indistinguishable
from a retriable call failure. Both now have a codec tag, alongside the `DOMException` that
`signal.throwIfAborted()` raises, so all three shapes of "the callee stopped because it was
cancelled" reach the caller with their type intact. Orleans has no counterpart, because
`OperationCanceledException` is a framework type its serializer already knows.

## An unresolvable error type carries its name, not its class

Orleans' `ExceptionCodec` writes the type name, message and properties for **every** exception and
resolves that name back to a `Type` on receipt, so a `catch (MyDomainException)` keeps working
across a silo boundary with no registration at all. Thresh cannot resolve an application class from
a name — nothing maps a wire string to a constructor, and doing so from untrusted input would be a
gadget. So `registerSurrogate` remains how an application type round-trips as ITSELF.

What is no longer lost is its name, message and own enumerable properties. Its `stack` and its
`cause` still are: the stack is a local artefact of the sending process, and `cause` is specified
as a NON-enumerable own property, so `Object.entries` does not reach it. Orleans carries both — a
remote stack trace via `ExceptionDispatchInfo.SetRemoteStackTrace`, and the inner exception
recursively — so this is narrower than `ExceptionCodec`, not equal to it. Both were left out to
keep every error response and every persisted state that happens to nest an `Error` from growing.

An `Error` subclass has no enumerable own
properties, so the codec's generic object branch used to flatten it to `{}` and the caller got a
bare `GrainCallError` carrying only the message — the type it discriminates on gone, with nothing
warning. `encodeValue` now has an `Error` branch (after the surrogate lookup and after the
cancellation family's own tags, so both still win) carrying `name`, `message` and the own
enumerable properties, and `decodeValue` rebuilds:

- one of **Thresh's own** error classes, or one of JavaScript's built-in ones (`TypeError`,
  `RangeError` and friends — the analogue of Orleans resolving the `System.*` namespace with no
  registration), matched against a closed table and what keeps `isThreshRuntimeError` and an
  `instanceof GrainCallError` narrowing firing cross-silo. The name alone never selects the
  constructor: the SENDER must also have proved `value instanceof` the class of that name, and only
  then does a `thresh` marker travel with it. Orleans gets that for free from an assembly-qualified
  type name; matching a bare `name` string would not, because an application is free to declare its
  own `class LimitExceededException` and a decoder trusting the string would rebuild Thresh's — a
  `ThreshRuntimeError` — so the caller's `isThreshRuntimeError` would answer true for a permanent
  domain failure and retry it;
- otherwise **`UnavailableExceptionFallbackException`**, upstream's own fallback class, carrying
  `errorType` (mirrored onto `name`, which is how JavaScript discriminates errors) and
  `properties`, also copied onto the instance so a transliterated `error.limit` reads.

Two consequences worth stating. The fallback is deliberately a plain `Error` and **not** a
`ThreshRuntimeError`, matching upstream's fallback deriving from `Exception` rather than
`OrleansException`: a domain error that classified as a Thresh transport failure would be retried
forever against a request that can never succeed. And a caller that previously received
`GrainCallError` for an unregistered remote error now receives the fallback instead — the
degradation moved from silent to recoverable, which is the whole point.

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

## A client leg listens; it is not duplex over its outbound connection

Orleans' client leg is **duplex over its own outbound connection**: the gateway answers a client on
the socket the client dialled, so a client needs no reachable address of its own and
`IGrainFactory.CreateObjectReference` costs nothing but a registration.

Thresh's `ClientNode` listens instead. Every socket here carries traffic one way — `connect()`
returns a send-only `Connection` whose only inbound read is the preamble ack — and the reply, or a
gateway's push to a client-hosted observer, travels over a **reverse connection to the peer's
advertised endpoint**. `InProcessTransport` has always worked that way (its reverse connection is
`network.deliver(from, ...)`, keyed by address), and `WebSocketTransport`'s accepted connection does
the same: it dials the endpoint the peer announced in its preamble rather than writing back down the
accepted socket, which would land on the dialler with nothing listening.

The cost of not being duplex is that a client needs an address a gateway can reach. It is not a
restriction on WHERE the seam works: a silo whose startup task calls
`GrainFactoryAccess.createObjectReference` provisions its embedded `ClientNode` leg on whatever
transport the silo itself uses, asking for an **ephemeral port** (`host:0`) on the silo's own host
and adopting the address the listener actually bound — `ClientNode.connect()` replaces its
configured `local` with `Listener.address` before it dials anything, so the endpoint it advertises
is the one it is really reachable on. That leg only ever has to be reachable from its gateway, which
is this silo in this process, because a call for a client-hosted object is routed to the client's
gateway silo first (`ClusterNode.routeToClient`) and never dialled directly by another silo.

`SiloBuilder.requireObserverHosting()` remains the declaration that a silo depends on the seam, and
still fails at `build()` rather than at first use if the configured transport cannot give the leg a
listener. Both transports the builder configures can, so it no longer rejects a
`WebSocketTransport`-hosted silo.

## The clock's fine reading is an epoch instant, not a stopwatch tick

Orleans reads time through `System.TimeProvider`, which pairs `GetUtcNow()` with
`GetTimestamp()`/`TimestampFrequency`/`GetElapsedTime()`. The fine half is an opaque, ORIGIN-FREE
tick, and Orleans uses it only to measure elapsed time — `ActivationRebalancerWorker` suspends
until `now + frequency * duration`, `GrainMigratabilityChecker` ages a cache.

Thresh's `TimeProvider` adds a single `nowNanos(): bigint` instead: the same wall clock as `now()`,
in nanoseconds since the Unix epoch. The pair collapses to one reading because the caller that
needed it does not measure an interval, it MINTS ORDERED VALUES from the clock — BeneDB's
sequencer mints MVCC revisions as epoch nanoseconds — and an origin-free tick cannot be one of
those. A millisecond is coarser than the interval between that grain's commits, so per-millisecond
values collide, the sequence falls to a synthetic increment, and the values stop being timestamps
while staying monotonic; anything later mixing the two (a GC floor of `min(head, now - window)`)
then lands on the wrong side.

`nowNanos` is **optional**, so every two-method structural implementation that predates it still
satisfies the interface. Read it through `nowNanosOf(time)`, which scales `now()` for a provider
without one — the fallback is millisecond-quantised, so a caller needing values distinct within a
millisecond needs a provider that implements the fine reading. Both of Thresh's do.

Two consequences worth naming. On `systemTimeProvider` the two readings come from different
sources — `Date.now()` and `performance.timeOrigin + performance.now()` — so `nowNanos` never
steps backwards but does not absorb a wall-clock correction made after process start, and can
drift from `now()`. That trade is deliberate for a caller minting ordered values, and it is the
same split Orleans has between `DateTimeOffset.UtcNow` and `Stopwatch`. On `FakeTimeProvider` the
fine reading is derived from the fake's own `current`, not the machine's, so `advance()` still
drives it and one fake instant reads the same value twice — the fake stays authoritative, which is
the obligation that comes with adding the reading at all.

## Additions beyond Orleans

A few capabilities layer on top of the faithful model without changing it: **durable journaling**
(a grain that journals each mutation and replays it on activation) and **durable jobs** (sharded,
at-least-once scheduled grain invocation). A further, **not-yet-built** direction is **browser state
replication** — a live read-view of grain state in the browser under a server-enforced trust model.
