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
- **A grain's key kind is stated as a value, and it types the key.** `"string"` (the default, which
  needs no statement), `"integer"`, `"guid"`, `"integer-compound"` or `"guid-compound"`, stated as
  `defineGrainInterface<ICounter, "integer">("ICounter", { key: "integer" })` (or `{ key }` on
  `defineGrain`); it determines the type of the `key` argument at every `getGrain`. The
  `GrainWith*Key` phantom markers still type the key, but a non-string kind that is only a marker
  or a type argument does not compile: it is erased at runtime, so the runtime could not read a key
  that arrived as text.
  As in Orleans, a grain id is its type plus the key text: the kind is not part of identity, and
  the grain type decides how the text is read. The declared kind is enforced by `getGrain`, used to
  read the key of an implicitly subscribed grain, and used to canonicalise every id before the
  catalog, directory or ring sees it, so `"005"`, `"+5"` and `5n` (or an upper-case guid string and
  the `Guid`) are one grain and one activation, and the grain sees its key as its declared kind
  whichever arrived first.
- **Serialization is registered at runtime** (`@serializable`) rather than source-generated;
  MessagePack is the default wire format.
- **Transport is WebSocket over HTTP**, behind an abstraction.
- **Single-threaded turns** are enforced by a per-activation turn scheduler. The guarantee is
  identical to Orleans; the mechanism differs because `await` yields the Node event loop.
- **A call-chain reentrancy section is a callback, not a disposable.** Orleans'
  `using var _ = RequestContext.AllowCallChainReentrancy();` (and `SuppressCallChainReentrancy`) is
  `await RequestContext.allowCallChainReentrancy(() => ...)` here: the section covers the
  callback's async extent and closes when its promise settles. The admission rule is Orleans' —
  off by default, and only the grain that opened the section admits a callback on that chain. One
  timing difference: opening a section admits a request on that chain that is already waiting at
  once, where Orleans admits it at the activation's next scheduling pass.
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
probe-graph failure detector. A silo watches the EndpointSlices of a headless `Service`, and the view
is a pure function of them, the same rule for every endpoint including the silo's own: `ready` is
`active`, not ready and `terminating` is `draining`, not ready otherwise is `joining`, and an
endpoint that is not listed is not a member. Silos run as a `StatefulSet` (stable ordinals keep a
restarted silo in the same ring position); liveness/readiness probes drive failure detection and the
graceful drain. Silo identity is `podName` + `podUid` rather than Orleans' `IP:port:generation`.

The status maps onto Orleans' as follows, with `ringSilos` as the one rule for who may own a range
or host a new activation:

- **The ring is the active members** (Orleans `DirectoryMembershipSnapshot`), and when no member is
  active it is the joining ones. That fallback has no Orleans counterpart — an Orleans silo becomes
  active through the membership table, not through a readiness probe that waits on it — and is what
  lets a cold cluster start: every silo sees the same joining set, so they agree on a ring and
  startup tasks can call grains; the first silo to become ready takes it over. The same rule feeds
  placement and migration candidates, the rebalancer and repartitioner, load publishing, the client
  gateway list, and stream, reminder and durable-job ownership.
- **A silo outside its ring creates no activation** (Orleans `Catalog.GetOrCreateActivation` creates
  only while the silo is active). A call that would activate a grain there is refused before it runs
  and its caller re-resolves; the activations it already hosts keep serving, and their directory
  entries stay valid because it is still a member. Stateless workers are gated too: such a silo
  serves a stateless-worker call with a worker it already hosts, without growing its pool, and
  otherwise sends the call to a silo in its ring (forwarded, when it arrived from a client), as
  Orleans' `StatelessWorkerDirector` places elsewhere when the local silo is not a compatible one.
  A joining silo's startup task that calls a stateless worker is therefore served by an incumbent,
  or locally in a cold start, where the joining silos are the ring.
- **Readiness asks whether the silo is listed**, not whether it owns a range: a joining silo must
  report ready for Kubernetes to mark it ready, which is what brings it into the ring.
- **The self-probe is answered locally**: the probe grain is activated through the node's local
  system-activation path, never placed or registered in the directory, so it measures this silo's
  own dispatcher on a silo that is not a placement candidate.
- **A silo that sees itself removed fences itself** (Orleans `MembershipTableManager.KillMyselfLocally`):
  once a view omits a silo that was listed, it fails liveness, refuses every call, stops its
  ownership services and abandons its activations without running deactivation hooks or
  unregistering them, since its peers have already dropped its entries. The kubelet then restarts
  it. Static membership never reports a removal and so never fences.

| Orleans mechanism | Replaced by |
| --- | --- |
| Membership table + gossip | Kubernetes API watch on Pod endpoints |
| Probe-graph failure detection | Liveness/readiness probes + endpoint removal |
| Silo generation counters | Pod name + UID |
| Gateway list provider (clients) | Kubernetes `Service` / DNS |
| Cluster discovery providers | The Kubernetes control plane |

### The membership version is the observed EndpointSlice revision

Orleans numbers views with the membership table's `MembershipVersion`, one counter every silo reads
from the same table. With no table, `MembershipSnapshot.version` under `KubernetesMembership` is the
highest EndpointSlice `resourceVersion` the silo's watch has observed: each listed slice's, and each
watch event's (a DELETED event carries the deletion's revision). For an etcd-backed resource that is
the etcd modification revision, one cluster-wide increasing counter, so silos that have seen the
same writes to the service's slices hold the same version and a silo whose watch lags holds a lower
one — the cross-silo ordering the directory's `staleView` guard and `awaitView` depend on. Because
the view is a pure function of the slices, two silos at one version hold the same ring. Kubernetes
documents `resourceVersion` as opaque; this relies on its etcd form. Every watch update must carry a
revision, and `KubernetesMembership` refuses one that does not: a change the watch cannot date (a
slice with a non-numeric `resourceVersion`) is not reported on its own, and is carried by the next
dated change.

The list's own `resourceVersion` is not used as the version: it is the cluster's global revision at
the moment of the list, which moves with writes to every other object, so a silo that listed later
would number an identical view higher and its peers would wait on a version they never reach. It is
used only when a re-list shows a change that no listed slice dates — a slice deleted while the watch
was down — and by `refresh`.

`MembershipService.refresh(version)` catches a silo up on demand (Orleans
`ClusterMembershipService.Refresh`): when a peer's directory operation carries a version this silo
has not reached, `ClusterNode.awaitView` asks for it. Under Kubernetes it waits briefly for the
watch, then re-lists; a list whose own `resourceVersion` is at least the requested version proves
that the slices it returned reflect every write up to that version, so the silo adopts the
requested version for the view the list gives. That covers a silo whose watch lags and a silo that
listed after a slice deletion it never saw, which holds a lower version than a peer that saw the
DELETED event: neither waits for an unrelated write to the slices. Watch events still in flight
when such a list runs ahead of them are older than it and are ignored. Static membership has one
view and resolves at once. Neither the version nor the view carries a rolling-upgrade compatibility
story; the project has no deployments to upgrade.

`MembershipService.updates()` has latest-value semantics like Orleans' `AsyncEnumerable<T>`, but
coalesces: an iterator that fell several views behind yields only the latest, where Orleans walks
every intermediate one. Every consumer re-reads `current()` to apply a view, and the host's
reconciliation loops until the node's applied version equals it, so no view is ever left unapplied.

## Functional / reducer authoring

Grains are authored as **factory closures** (`defineGrain` + hooks like `usePersistentState`) rather
than classes with attributes; the Orleans-style class form is retained underneath as the substrate
and interop surface. **Reducer** and **single-dispatch** grains (`defineReducerGrain`) add an
event-folding authoring shape on top. These change how a grain is *written*, not what it *is* — the
runtime, guarantees, and lifecycle are unchanged.

### Rules of hooks

`usePersistentState`, `useReducerState`, `useTransactionalState`, `useDurableState` /
`useDurableDictionary` / `useDurableList` / `useDurableQueue` / `useDurableSet`,
`useDurableJobHandler`, `useContext`, `useMayInterleave`, `useGrainFacet`, `useOnDehydrate` /
`useOnRehydrate` and `useDependency` take **no context argument**. They resolve the activation
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

### The two forms stay in lockstep

Orleans has one authoring form (a class, with attributes and constructor injection), so the
class-only features (`[MayInterleave]`, facet attributes, regex channel subscriptions, migration
participation, constructor-injected services) each need a functional spelling here. They are: 
`useMayInterleave`, `useGrainFacet`, the `regexImplicitChannelSubscriptions` option,
`useOnDehydrate` / `useOnRehydrate` and `useDependency`. Departures from the class spelling:

- **Per-silo dependencies are tokens, not constructor parameters.** Orleans resolves a grain's
  constructor from DI; a class grain here gets the same through a `GrainActivator`, and a functional
  grain asks `useDependency(token)` for a value registered with `SiloBuilder.addGrainDependency`.
  A token is compared by identity and carries the type.
- **The may-interleave predicate is per activation** for a functional grain (it is built inside the
  factory), and per class for `@mayInterleave`. The admission rules are identical.
- **Migration hooks are not methods of the grain.** They are installed as non-enumerable members
  that the call surface refuses by name, so a functional grain does not expose them to callers.

A feature matrix test (`packages/core/src/authoring-matrix.test.ts`) lists every decorator and hook
and fails when one is added to one form without an entry for the other. Deliberately single-path:
`JournaledGrain` and its custom-storage hooks (class), `defineReducerGrain` (functional).

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
same-silo call, which does not serialize unless `wireSemanticsForLocalCalls` is on.

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

What is carried is its name, message, own enumerable properties — and now, as of #63, `stack` and
`cause` too, closing the two gaps #61 deliberately left open.

`cause` round-trips recursively: it is checked for with `in` rather than a truthiness test (so an
explicit `cause: undefined` still crosses as a present own property, not an absent one), encoded
through the same element path an array member takes, and installed on the rebuilt error as a
NON-enumerable own property via `Object.defineProperty` — matching the spec's own
`CreateNonEnumerableDataPropertyOrThrow` install, so a rebuilt error is indistinguishable from
`new Error(m, { cause })`. A nested `Error` cause decodes through the same `error` case again, so a
cause chain rebuilds link by link, each as its own class; a cause cycle (`err.cause === err`) is
caught by the same `CircularReferenceError` guard a cycle anywhere else in the graph gets.

`stack` travels as a plain, opaque string — never parsed or restructured — capped at 8KB with a
truncation marker, and installed as a **remote** stack trace: the sender's frames are kept
verbatim and a trailing marker line (`--- End of remote stack trace from grain call ---`) is
appended, the analogue of `ExceptionDispatchInfo.SetRemoteStackTrace` (which appends .NET's own
"End of stack trace from previous location" line). The receiving process's own frames are
deliberately **not** appended — the rehydration point is noise, not signal.

`AggregateError.errors` — likewise a non-enumerable own property `Object.entries` never reached —
round-trips too: a genuine `AggregateError` is now in the closed built-ins table (alongside
`TypeError` and friends) and rebuilds as itself with its member errors, `cause` and `stack` all
decoded exactly as any other error's; a subclass with its own `name` still takes the
`UnavailableExceptionFallbackException` fallback, but keeps its decoded `.errors` array rather than
losing it.

Getting `AggregateError` onto this same path — rather than a separate, lossier one — took a second
fix. `ClusterNode` already had a dedicated `aggregateMessages` side channel predating #63, built for
a non-fire-and-forget broadcast publish's aggregated subscriber failures: it carried only the member
errors' `.message` strings, no types, no `cause`, no `stack`. When #63 first landed, `serializeError`
still special-cased every `AggregateError` into that side channel BEFORE reaching this codec path at
all, so a grain that threw an `AggregateError` directly never got the fidelity described above — it
still collapsed to bare `Error(message)` members, and silo-to-silo calls didn't even read
`aggregateMessages`, so `.errors` disappeared entirely and the caller saw a plain `GrainCallError`.
`serializeError` now always tries this codec path first for an `AggregateError`; `aggregateMessages`
is sent alongside it, not instead of it, purely so a peer that predates this fix (and still speaks
only the old side channel) gets at least the message-only reconstruction. Every peer in this
codebase prefers the codec path and ignores `aggregateMessages` when it is present. The one place the
side channel still matters on its own is if the codec genuinely cannot encode the `AggregateError` or
one of its members (something uncodable nested inside) — then the response falls back to
`aggregateMessages` alone, and the caller gets an `AggregateError` whose members are bare
`Error(message)` stand-ins with no type, `cause`, or `stack`.

Two things stay narrower than `ExceptionCodec` even after this. There is no analogue of `HResult`
or `.Data` — Orleans' `SetBaseProperties` restores both alongside the inner exception, and Thresh
carries neither. And type resolution is unchanged: an application error still round-trips as
itself only through the closed built-ins table or a registered `registerSurrogate`, never from the
wire's bare `name` string, for the same gadget-and-spoofing reasons the previous paragraph gives.
Payload growth was the reason `stack` and `cause` were left out originally; the cap bounds a single
stack, but a long cause chain of stack-bearing errors still multiplies it, which is worth watching
if it proves heavy in practice. Sender-side file paths and frames also now cross the wire to
callers — including external clients via the gateway — the same trade Orleans makes.

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

## The service dimension covers every durable store, not just grain state

#59 added a `serviceId` dimension (Orleans' `ClusterOptions.ServiceId`, kept independent of
`ClusterId` because it "should survive deployment and redeployment, where as ClusterId might not" —
`ClusterOptions.cs:36`) to `PostgresGrainStorage` and `RedisGrainStorage` only. #64 carries the same
dimension to every other durable store that partitions by table name or key prefix alone:
`PostgresReminderTable`, `RedisReminderTable`, `RedisJournalStorage`, `RedisTransactionalStorage`,
the Postgres and Redis stream registries, cursor stores **and queues** (partitioning only the
registry/cursors and not the queue itself would still let two services sharing one queue
cross-deliver each other's events). The default is `DEFAULT_SERVICE_ID` (`"default"`,
`@thresh/core/default-service-id`) everywhere, never `clusterId`, for the same reason #59 settled on
it: a cluster-id default would strand state on a redeployment that changes the cluster id.

No store migrates data written under an earlier layout: each Postgres store creates its table in
its current shape, and a table or key left by an older Thresh is not read. Thresh has no deployed
state to carry forward, so a layout change is a clean break.

## A clustered silo refuses settings only one silo can hold

Orleans accepts `AddMemoryGrainStorage`, `UseInMemoryReminderService` and development clustering on
any silo and documents them as development-only. Thresh refuses them at `build()` on a silo whose
membership is anything but a static view of itself alone, because each fails silently there: an
in-memory grain or journal store is empty when an activation moves, a reminder registered on a silo
that does not own its grain's range is never fired by the silo that does (that silo reads its own
table), and a static multi-silo view keeps a departed silo `active`, so calls routed to it hang.
Each refusal names its durable alternative and its explicit opt-in: `allowSharedInMemoryState()` for
in-memory instances every silo shares in one process (`TestCluster`), `allowWithoutLiveness` on
`useStaticMembership`, and `allowInCluster` on the in-memory storage backend for disposable state.
A store the builder constructs itself is one per silo by construction, so only `allowInCluster`
admits it.

## Storage clients start below the node, and a failed start rolls itself back

The silo runs Orleans' staged lifecycle (`ServiceLifecycleStage`, `LifecycleSubject`): every
subsystem starts at a named stage ascending and stops descending, and a participant whose stop
throws is logged while the rest still stop. Three placements differ from Orleans. Storage clients
(Redis, Postgres, Kafka metadata) and the membership watch sit at `RuntimeInitialize`, below the
node at `RuntimeServices`, where Orleans initialises storage providers at `RuntimeStorageServices`
above its messaging: here the node listens as soon as it starts and a peer's call can activate a
grain at once, so the client a grain reads must already be connected, and it must close only after
the node has deactivated every activation. The readiness flip is a participant at `Last`, so it
publishes first on stop. The membership reconcile, which tells reminders, durable jobs and pulling
stream agents which ranges they own (Orleans' ring-change listeners, which have no stage of their
own), sits one stage above `Active`, so it stops before every service it feeds; from the moment
stop begins it runs no ownership hook, and its stop waits, within the stop budget, for a reconcile
already in flight. Orleans' `LifecycleSubject.OnStart` does not itself unwind on failure; the
host does, calling `onStop` and rejecting with the original error, so a half-started silo leaves
nothing listening, connected or armed. Three further departures keep that guarantee. Within one
stage a stop runs the participants one at a time in the reverse of registration order, where
Orleans stops a stage's observers concurrently, so the whole stop order is the reverse of the start
order. A stop issued while a start is in flight halts the start (no later stage begins) and stops
what began, where Orleans leaves the start running. And a participant whose stop has not settled
within the stop budget is logged as failed and abandoned, so one hung stop cannot hold the rest of
the stop (or a rollback) forever; Orleans bounds this through the host's shutdown timeout.

## A transaction resource delays deactivation for a bounded time

As in Orleans (`ActivationLifetime.BlockDeactivation` and `OnStopAsync`), a transactional-state
resource that holds a transaction lock, has a transaction in its commit queue, or is still confirming
a commit only delays a deactivation that has begun: after `onDeactivate`, the activation's
deactivation pipeline polls every 10 ms, on the silo's clock, for up to five seconds for the block to
clear, then proceeds. Every path honours it: idle collection, a forced collection, migration and
rebalancing, an explicit `deactivateOnIdle` and silo shutdown. Calls that arrive meanwhile are held
and rerouted once the activation has gone, as Orleans holds them, so the wait ends early only through
what the resource itself drives: its commit queue, the lock timeout, its pings and confirmations. A
resource deactivated after the five-second bound loses its lock, or leaves its prepared record or
commit record for the next activation to load and resume.

## The transaction protocol departs from Orleans in a few places

The commit protocol is Orleans': the agent sends a one-way `prepare` to every participant but the TM
and awaits `prepareAndCommit` on the TM, or `commitReadOnly` on every participant of a read-only
transaction; the TM's commit queue collects the votes, writes the commit with its own state, answers
the agent, confirms or cancels the participants, and removes the commit record once all have
confirmed; a prepared participant pings its TM at `RemoteTransactionPingFrequency`; pending states
form a chain. These are the places it differs:

- **The TM answers no ping while it still holds the transaction in its lock.** Orleans' TM presumes
  an abort for any transaction it does not find in its commit queue or its confirmation worker, which
  is safe only while the ping frequency (60 s) exceeds the lock timeout (8 s): a `prepareAndCommit`
  that arrives later would otherwise commit a transaction a participant has already dropped. Here the
  TM stays silent until its lock times out, and the next ping is answered.
- **`TransactionCommitter` is outside the protocol.** Upstream's committer has its own storage-backed
  commit queue and is elected TM ahead of every writer, running its operation as a precondition of
  the commit write, so a failing operation aborts every participant. The port's committer is
  memory-only and cannot be reached by participant id: the agent commits it after the elected TM has
  made the commit durable, and aborts it otherwise. A failing operation therefore leaves the other
  participants committed and the caller sees `TransactionInDoubtError`, which is what the ported
  `TocFaultTransactionMemoryTests.MultiGrainWriteTransactionWithCommitException` asserts, where
  upstream asserts the participants rolled back. A transaction whose only writer is a committer
  commits it directly.
- **No deactivation after repeated storage failures.** Orleans deactivates the grain after ten
  consecutive storage failures, or after a storage conflict (an etag mismatch from a duplicate
  activation). The commit queue here aborts what it holds and restores from storage each time, but
  asks for no deactivation.
- **A storage batch that will never be written fails what waits on it.** Orleans drops such a batch
  silently, so a commit-record collection waiting on it waits for ever; here the collection retries
  after `confirmationRetryDelayMs`.
- **On load, prepared records are queued again in sequence order**, where Orleans orders them by time
  stamp. The agent does not merge the participants' clocks into the transaction's time stamp
  (Orleans `TransactionInfo.RecordRead`/`RecordWrite` and `MergeUtcNow`), so time stamps from
  different TMs are not ordered causally here; the sequence ids are, because each pending state is
  built on the one before it.
- **A transaction leaves the lock the moment its role is known.** The lock is the port's wait-die
  `ReaderWriterLock`, not Orleans' lock groups, so a reader whose role is known enters the commit
  queue at once, where Orleans releases group members only in time-stamp order. Only reads are
  reordered this way; a writer holds the lock alone, so the chain of pending states is unaffected.
- **A `prepared` vote that arrives before the agent's `prepareAndCommit` is kept by transaction id**,
  where Orleans keys it by time stamp. As upstream, such votes for a transaction the TM never hears
  of again are never discarded.

## Resource-optimized placement scores raw readings over every candidate

`ResourceOptimizedPlacement` ports the scoring of Orleans' `ResourceOptimizedPlacementDirector`
(normalised weights over CPU, memory usage, available memory, maximum memory and activation count,
the local-silo preference margin, overloaded silos left out). Two things differ. The readings are
the raw ones each silo pushes in its load snapshot, where Orleans scores Kalman-filtered CPU and
memory, so a single noisy sample moves the score more here. Every eligible candidate is scored,
where Orleans scores a random square-root-of-n subset. A silo that has pushed no snapshot is skipped, as in Orleans; only a context with no statistics source at all falls back
to activation counts. The default statistics source is Node's
event-loop utilisation for CPU and process and OS memory for the rest, and a silo accepts any
`EnvironmentStatisticsProvider` in its place (Orleans' `IEnvironmentStatisticsProvider`).

## The activation lifecycle follows Orleans, with four differences

As in Orleans' `ActivationData`, an activation has one lifecycle state, and activating,
deactivating and dehydrating run on a queue of lifecycle operations that starts only when no
request is running and admits no request while one is pending, on a reentrant grain too. A
deactivation request (`deactivateOnIdle`, an escaped `InconsistentStateError`, `migrateOnIdle`,
idle collection, a silo stop) moves the activation to deactivating at once: calls and stream
deliveries that reach it are held and rerouted once it has gone, and timer ticks are dropped. Four
things differ:

- An activation that calls `deactivateOnIdle` from its own `onActivate` refuses the calls waiting on
  it with "activation unavailable" instead of rerouting them, which is what Orleans' own
  `DeactivateOnIdleWhileActivate` test observes as "Forwarding failed".
- An explicit memory-stream subscription delivers through a handler closure rather than a message
  to the grain, so a delivery held on a deactivating activation is released with a reroute
  rejection and the provider redelivers it to the handler the next activation resumes. The
  subscriber is not told of an error.
- `ActivationTimeout` (`SiloConfig.activationTimeout`, 30 s by default as in Orleans) fails an
  activation whose state load and `onActivate` have not finished in time, rejecting its waiting
  calls and removing it so a later call activates afresh. Orleans cancels `OnActivateAsync` through
  its cancellation token; `onActivate` has no token to observe, so a hung one keeps running in the
  background after the activation has given up on it.
- A deactivation that cannot start because a request never finishes is abandoned once it has waited
  `maxRequestProcessingTimeMs`, even when no call is waiting on it, and whoever awaits it is
  released. Orleans checks for a stuck deactivation only when a message arrives, so with no message
  arriving it never declares one stuck; here the waiting deactivate operation counts as the
  message, as the `onDeactivate` turn queued behind the wedged one did before lifecycle operations
  had their own queue.

## A same-silo call shares its arguments unless asked not to

Orleans deep-copies a call's arguments and its response even when caller and callee share a silo
(unless a type is `[Immutable]`), so placement never changes what a callee can see or mutate.
Thresh hands a same-silo callee the caller's own objects, and an error keeps its class, while the
same call to another silo round-trips the value codec. `SiloConfig.wireSemanticsForLocalCalls`
(and `TestClusterOptions.wireSemanticsForLocalCalls`) restores placement-independence by passing
the arguments, the result and a thrown error of every call through that codec round trip. The codec
is the copier here, so the option also flattens or refuses class instances and rebuilds error
classes exactly as the wire does. It is off by default in a silo and on by default in
`TestCluster`, so a test sees what a cross-silo call would.

This is an open choice, not a settled one. Copying in production would match Orleans and make
placement invisible to grain code, at the price of an encode and a decode on every local call,
a loss of class identity on arguments and results (a class instance with no surrogate arrives
flattened, an unregistered error class arrives as `UnavailableExceptionFallbackException`), and
a refusal for values the wire cannot carry, such as a cyclic argument or a function. Not copying
keeps local calls cheap and lets a grain pass its own objects to a neighbour, at the price of
aliasing no cross-silo call has: a callee can mutate its caller's argument, a caller can mutate
a grain's returned state, and a suite that lands caller and callee together passes where a
multi-silo deployment fails. The production default stays off.

## A process epoch rides the preamble and the correlation, not the silo address

Orleans carries a generation in `SiloAddress`, so a restarted silo at the same endpoint is a
different address, and a message meant for its predecessor is recognised as stale. Thresh's
`SiloAddress` keeps its identity (`podName` + `podUid` + endpoint, no generation), so the
generation lives elsewhere: each process picks a random epoch at load (`PROCESS_EPOCH`), carries it
in every connection preamble it dials, and stamps it on every request it awaits; the response
carries it back. A pending call completes only on a response with its own id, this epoch, and
arriving on the connection the request went out on. A late reply meant for a container's previous
incarnation therefore completes nothing in its successor, even though correlation ids restart at 1
in every process. Such a response is dropped and counted (`thresh.messaging.response.misrouted`).

## Each silo dials its own connection for the requests it sends

Replies travel down the connection their request arrived on, as in Orleans, so a silo never dials
a peer to answer it. Orleans also registers an accepted silo connection in its connection manager,
so a silo may send its own requests to that peer over the connection the peer opened. Thresh does
not: a silo sends its requests over a connection it dialled, one per peer, so two silos that call
each other hold one connection in each direction, each carrying one side's requests and the other
side's replies. A caller detects the loss of the connection its call went out on; that is the only
connection its reply can arrive on.

## Additions beyond Orleans

A few capabilities layer on top of the faithful model without changing it: **durable jobs**
(sharded, at-least-once scheduled grain invocation). A further, **not-yet-built** direction is
**browser state replication** — a live read-view of grain state in the browser under a server-enforced
trust model. Durable journaling is not an addition: Orleans 10 ships it as `Orleans.Journaling`
(`DurableGrain`, `DurableValue`, `DurableDictionary`, `DurableList`, `DurableQueue`, `DurableSet`),
and `@thresh/journaling` ports it; its departures are listed in the next section.

## Durable collections buffer until `writeDurableState()`

As in `Orleans.Journaling`, a durable collection mutation is a synchronous in-memory operation that
buffers its log entry, and `writeDurableState()` (`Grain`) or the function `useWriteDurableState()`
returns (hooks) is `WriteStateAsync`: one append of everything buffered across the grain's
structures. A failed write, a lost-writer conflict and deactivation with entries still buffered
behave as in `JournaledStateManager` (see the state guide). The departures:

- Orleans' `DurableValue` coalesces repeated sets into one entry per write through a dirty flag;
  here every `set` buffers its own entry, so ten sets cost ten entries in one append.
- Orleans takes a snapshot work item instead of an append when compaction is requested. Here the
  threshold compaction runs after a write that leaves nothing buffered, and is skipped, not forced,
  while entries are pending, so a `JournaledGrain` sharing the log can defer it until the grain
  next writes.
- A `JournaledGrain` on the shared log writes its events at `confirmEvents`, immediately; Orleans
  has no equivalent, and that write does not flush the durable collections.
- Orleans' recovery after a lost writer runs in the manager's work loop; here it runs at the start
  of the next `writeState`, which is the same moment from the grain's point of view.

## JournaledGrain history is snapshot-plus-tail unless retention is opted in

Orleans' log-storage consistency provider keeps the whole log and never snapshots it; its
state-storage provider snapshots and keeps no events. A Thresh `JournaledGrain` on the journal
substrate shares one log (and one snapshot threshold) with the grain's durable collections, so by
default it behaves like the state-storage provider: each compaction truncates the log to a snapshot
and the activation holds only the events since it. Asking for events below the snapshot throws an
error naming the `retainEvents` option. Whole-log retention (the log-storage provider's behaviour)
is the opt-in (`retainEvents: true`, or `retainJournaledEvents: true` in the silo config): each
compaction then writes the confirmed events as a retained-history frame ahead of the snapshot
frames. Until the storage seam can snapshot without truncating, that costs a full rewrite of the
history at every compaction (quadratic in events over a grain's lifetime) and holds it in memory,
where Orleans' log storage appends, so it is off by default.
