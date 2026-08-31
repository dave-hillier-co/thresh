# Porting an Orleans application to Thresh

A mechanical reference for translating a .NET + Orleans codebase into TypeScript on Thresh.
It is written to be usable as the standing instruction for an automated per-file port: every
row is a substitution a transliterator can apply without judgement, and every "no direct
equivalent" entry names the redesign the porter must make instead.

For how Thresh's *design* differs from Orleans, see [`deviations.md`](deviations.md). This
document is narrower: it is about moving code across.

## The order of work

Port along the dependency tree, leaves first, and port each layer's tests before its
implementation. Pure value types and pure algorithms come first because they need no runtime;
grains come last because they are the only code whose behaviour depends on Thresh itself.
The layers of a typical Orleans service:

1. Value types, DTOs, exceptions — no framework surface at all.
2. Pure domain algorithms — parsers, compilers, folds, evaluators.
3. Grain interfaces + the serializable types crossing them.
4. Grain implementations.
5. Hosting and the external protocol surface.

A test that fails for its own reason is worth more than one that fails because a layer beneath
it is missing, so a layer is only "ported" when its tests pass against the ported code.

## Grains

| Orleans | Thresh |
|---|---|
| `IGrainWithStringKey` | `GrainWithStringKey` (from `@thresh/core/grain`) |
| `IGrainWithGuidKey` / `IGrainWithIntegerKey` | `GrainWithGuidKey` / `GrainWithIntegerKey` |
| `public interface IFoo : IGrainWithStringKey` | `interface IFoo extends GrainWithStringKey` **plus** `const IFoo = defineGrainInterface<IFoo>("IFoo")` — the runtime needs a value, not just a type |
| `class FooGrain : Grain, IFoo` | `const FooGrain = defineGrain<IFoo>("Foo", (ctx) => { ... return { ... } })` |
| `Task` / `Task<T>` return types | `Promise<void>` / `Promise<T>` |
| `ValueTask<T>` | `Promise<T>` — there is no separate cheap-completion type |
| `OnActivateAsync(CancellationToken)` | an `onActivate(reason)` member **returned from the factory**, not registered on `ctx` |
| `OnDeactivateAsync(reason, token)` | an `onDeactivate(reason, signal?)` member returned from the factory |
| private fields on the grain class | closure variables in the factory — per-activation, reset on reactivation |
| `this.GetPrimaryKeyString()` | `ctx.id.key` |
| `this.GetGrainId()` | `ctx.id` (a `GrainId`; `id.type`, `id.key`, `toString()` gives `"Type/key"`) |
| `DeactivateOnIdle()` | `ctx.runtime.deactivateOnIdle()` |
| `DelayDeactivation(ts)` | `ctx.runtime.delayDeactivation(duration)` |
| `GrainFactory.GetGrain<IFoo>(id)` | `ctx.getGrain(IFoo, id)` inside a grain; `runtime.getGrain(IFoo, id)` elsewhere |
| `[StatelessWorker]` | `defineGrain(..., { stateless: true })` |
| `[Reentrant]` | `defineGrain(..., { reentrant: true })` |
| `[AlwaysInterleave]` on a method | `defineGrainInterface("IFoo", { options: { fooMethod: { alwaysInterleave: true } } })`. Not `readOnly`, which interleaves only when BOTH the blocking and the incoming turn are read-only, so it does nothing against a non-read-only writer. |
| `[OneWay]` on a method | the same `options` map entry: `{ oneWay: true }` |
| a per-method attribute on an **inherited** interface member (`interface IFoo : IBar` where `IBar` declares `[AlwaysInterleave] ReadFrom`) | **repeat the option on the derived interface's own `options` map.** Thresh's per-method options live on the concrete `GrainInterface` VALUE and are not inherited from an extended TypeScript interface, so the flag is silently dropped otherwise — an interleaving read parks behind an in-flight write, which is a deadlock or a timeout storm no unit test shows. The base fragment, having no `defineGrainInterface` value of its own, gets none. |
| `public const long Key = 0` on a grain interface | a module constant (`const FOO_GRAIN_KEY = 0n`), name-folded per the static-class rule — a TypeScript interface cannot carry one. `GrainWithIntegerKey`'s key type is **bigint**, so the literal is `0n`, never `0`. |
| `[Immutable]` on a type crossing a grain boundary | **nothing** — a same-silo call in Thresh passes arguments and replies BY REFERENCE with no serialization or clone step, so the hot-path property `[Immutable]` bought in Orleans holds by default. The divergence runs the other way: Orleans deep-copies a NON-immutable argument on a local call and Thresh does not, so a ported grain that mutates an argument, or a caller that mutates a reply, now corrupts the other side's object. Freeze or copy at the boundary where the C# relied on Orleans' copy. |
| `IIncomingGrainCallFilter` / `IOutgoingGrainCallFilter` (a CLASS with an `Invoke(context)` method) | a **function** — `IncomingGrainCallFilter` / `OutgoingGrainCallFilter` from `@thresh/core/grain-call-filter`, typed `(context) => Promise<void>`. Port each C# filter class as a FACTORY returning that function, with the constructor's dependencies becoming the factory's parameters. The filter's method matching also changes shape: Orleans matches by reflection on `context.InterfaceMethod` (`method.DeclaringType == typeof(IFoo) && method.Name == nameof(IFoo.Bar)`), while Thresh's `GrainCallContext` carries `interfaceName` / `methodName` STRINGS. Compare against `IFoo.name` and the **TypeScript** method spelling (`"bar"`, not the C# `"Bar"`), and derive the method name from the interface type (`keyof IFoo & string`) rather than hard-coding it: a casing slip makes the filter a silent no-op on every call, which no type check catches and which surfaces only as the missing behaviour the filter existed to add. |
| `IGrainObserver` | `ClientNode.createObjectReference(IObs, impl)` / `deleteObjectReference(ref)` (`@thresh/client/client-node`), and the same pair on the `GrainFactoryAccess` a startup task is handed. The reference is passed into a grain as an ordinary grain-call argument and the grain calls back on it; `packages/parity/src/default-cluster/observer.test.ts` runs the pattern end to end. Combine with `ObserverManager` (`@thresh/core/observer-manager`) for the snapshot/TTL fan-out collection. ONE DIFFERENCE, and it is load-bearing wherever a subscription is REFRESHED: Orleans observer references have value equality, so `ObserverManager.Subscribe(observer, observer)` keys the collection by the reference itself; Thresh references do not, so key by `grainReferenceIdentity(observer).grainId.toString()` (`@thresh/core/grain-reference`) instead — keying by the reference makes every heartbeat resubscribe ADD an entry instead of refreshing one, and the set then grows without bound. |

The class + `@grain()` decorator form still exists as an interop surface, and it transliterates
one-for-one from an Orleans grain class. Prefer it for the *first* pass on a large grain — the
diff against the C# stays readable — and convert to `defineGrain` once its tests are green.

## State and persistence

| Orleans | Thresh |
|---|---|
| `[PersistentState("name", "provider")] IPersistentState<T>` | `usePersistentState<T>(ctx, "name", { provider, defaultValue })` |
| `state.State` | `state.value` |
| `state.WriteStateAsync()` | `state.write()` |
| `state.ReadStateAsync()` | `state.read()` |
| `state.ClearStateAsync()` | `state.clear()` |
| `state.Etag` | `state.etag` |
| `state.RecordExists` | `state.exists` |
| `InconsistentStateException` | `InconsistentStateError` (`@thresh/core/errors`) |
| `IGrainStorage` | `GrainStorage` (`@thresh/core/grain-storage`) |
| `AddMemoryGrainStorage` / `AddRedisGrainStorage` | `useMemoryStorage()` / `addRedisStorage()` on the silo builder |

`JournaledGrain<TState, TEvent>` maps to Thresh's `JournaledGrain` with the same shape —
`raiseEvent` / `confirmEvents` / `state` / `tentativeState` / `version` — backed by a
`LogViewAdaptor`. `TransitionState(state, event)` becomes the `transition` function passed to
`bindJournaledGrain`.

`ICustomStorageInterface<TState, TDelta>` — a grain that owns its own log persistence rather than
delegating to the journal substrate — maps to `CustomStorageInterface` from
`@thresh/core/journaled-grain`. Implement it *alongside* `JournaledGrain` and the binder routes
the grain to the custom-storage adaptor automatically, leaving the journal substrate untouched;
this is the fork Orleans makes by configuring the CustomStorage log-consistency provider.

`RaiseConditionalEvent(event)` — the Orleans conditional append that returns `false` when the
confirmed version moved under it — has **no Thresh counterpart**. `raiseEvent(event)` followed by
`await confirmEvents()` is the pair, and the adaptor's own compare-and-set on the version plays the
role of the condition: a lost race surfaces as an `InconsistentStateError` out of `confirmEvents`,
so the C#'s `if (!raised)` branch becomes a `catch` narrowed to that class. Anything else thrown is
a genuine storage failure and must propagate — a bare `catch` here silently converts a failed write
into "someone else won the race", which callers retry against a base that never received the write.

Supply `readStateFromStorage()` (returning `{ version, state }`),
`applyUpdatesToStorage(updates, expectedVersion)` (returning whether the CAS held) and
`clearStoredState()`. Two things differ from Orleans and both are deliberate:

- **`retrieveConfirmedEvents` is unsupported.** The adaptor keeps a view, not a log. Orleans is
  the same — `CustomStorageAdaptor` leaves `RetrieveLogSegment` at its `NotSupportedException`
  base — so a grain that needs its log back reads it from the storage it writes.
- **The CAS retry is bounded**, defaulting to 5 attempts before an `InconsistentStateError`.
  Orleans retries forever because its retry runs on a background protocol loop; Thresh awaits
  `confirmEvents()` inside the grain turn, where an unbounded retry would hang the activation
  until the stuck-turn watchdog fires. The events stay pending, so a later confirm retries them.

## Reminders and timers

| Orleans | Thresh |
|---|---|
| `IRemindable` | `Remindable` (`@thresh/core/reminder`) — return a `receiveReminder(name, status)` member from the factory |
| `ReceiveReminder(name, TickStatus)` | `receiveReminder(name, status: TickStatus)` |
| `RegisterOrUpdateReminder(name, dueTime, period)` | `ctx.runtime.registerReminder(name, due, period)` |
| `UnregisterReminder(reminder)` | `ctx.runtime.unregisterReminder(name)` — by name, not by handle |
| `GetReminder(name)` / `GetReminders()` | `ctx.runtime.getReminder(name)` / `getReminders()` |
| `RegisterGrainTimer(callback, dueTime, period)` | `ctx.runtime.registerTimer(callback, due, period)` |
| `TimeSpan.FromSeconds(30)` | the `Duration` object literal `{ seconds: 30 }` (`@thresh/core/duration`); `durationToMs` converts. There are no `seconds()`/`minutes()` helper functions. |
| `TimeProvider` injected into a grain's constructor | `ctx.runtime.timeProvider` (Orleans `IGrainRuntime.TimeProvider`) — the silo's configured clock, the same one `registerTimer` schedules against. Read it for any time-based state the grain owns itself (an `ObserverManager`'s TTL expiry, a staleness check) rather than importing `systemTimeProvider`: pinning to the system clock is what makes a `TestCluster` built with a `FakeTimeProvider` unable to drive that expiry without sleeping in real time. |
| `timeProvider.GetTimestamp()` / `TimestampFrequency` | `nowNanosOf(ctx.runtime.timeProvider)` (`@thresh/core/time-provider`) — the same clock as `now()`, read as epoch nanoseconds. Orleans' fine reading is an origin-free stopwatch tick for measuring elapsed time; Thresh's is an absolute instant, because what needed it mints ORDERED VALUES from the clock and a millisecond is coarser than the interval between them. Call it through `nowNanosOf`, not `timeProvider.nowNanos!()`: the member is optional so older structural clocks still typecheck, and the helper supplies the (millisecond-quantised) fallback. See [`deviations.md`](deviations.md). |

## Serialization

Orleans requires `[GenerateSerializer]` + `[Id(n)]` on every type crossing a grain boundary.
Thresh serializes plain objects structurally, so **the attributes simply disappear** — a C#
record with `[GenerateSerializer]` becomes a TypeScript `interface` or a `readonly` type alias
and nothing else is needed.

The exceptions, where the port must do real work:

- **Types with behaviour that must survive the wire** (anything the receiver calls methods on)
  need a surrogate registered with the versioned serializer. Orleans' `[RegisterConverter]` /
  `IConverter<T, TSurrogate>` maps onto the surrogate registry.
- **Exceptions crossing a grain boundary.** Orleans needs `[GenerateSerializer]` on the
  exception type; Thresh needs the error registered so it round-trips as its own class rather
  than as `UnavailableExceptionFallbackException`. Grep the C# for `[GenerateSerializer]` on
  anything deriving from `Exception` — each one is a registration in the port — and add one for
  any type a caller discriminates on with `instanceof`, whether or not the C# marked it.
- **Google.Protobuf `CalculateSize()`.** The SERIALIZED byte length of a message, and where a
  size limit is compared against it that length is wire-visible. In ts-proto it is
  `Msg.encode(m).finish().length` — never `JSON.stringify(...).length` and never a UTF-16 char
  count, both of which disagree with it for any multi-byte or numeric field. A ts-proto field
  of well-known type `Struct` arrives auto-unwrapped as a plain object, so size it by wrapping
  it back first: `Struct.encode(Struct.wrap(ctx)).finish().length`. Size exactly the message the
  C# sized — a limit named for a field is often applied to the whole enclosing message, and
  "fixing" that changes which requests are rejected.
- **`byte[]` and text encoding.** `byte[]` becomes `Uint8Array`; `Encoding.UTF8.GetBytes` /
  `GetString` become `new TextEncoder().encode(...)` / `new TextDecoder().decode(...)`. Note that
  `Uint8Array` compares by reference, so any byte array inside a value type needs explicit
  content equality. Binary inside a value type round-trips as itself on every path: the MessagePack
  serializer and the in-memory clone carry the `Uint8Array` natively, while `serializeValue` and the
  JSON serializer tag it as base64 (`encodeValue(value, { binaryAsBase64: true })`) — JSON has no
  binary type, and untagged a typed array degrades to `{"0":1,...}` and comes back a plain object.
- **`decimal`, `long`/`ulong`, `DateTimeOffset`, `Guid`.** TypeScript `number` is a float64.
  `long`/`ulong` that can exceed 2^53 must become `bigint` or a string, and the choice has to be
  made once, at the value type, not per call site. `uint`/`ushort` fit in `number` exactly, so
  what is lost is the range invariant the C# type system enforced — restore it with a guard at
  the constructing factory. `DateTimeOffset` becomes an ISO string, epoch-millis `number`, or
  epoch-nanos `bigint`; pick one per field and write it down, and note that .NET's 100ns tick
  resolution (seven fractional digits) survives none of `Date`, epoch-millis, or `number`.
  `Guid` becomes `string`.
- **`DateTimeOffset.UtcNow` as a clock READ, not a stored value.** Where the C# reads the clock to
  MINT a value that must be greater than the last one — a revision, a sequence stamp, a GC floor —
  `Date.now()` is not the substitution. It resolves to the millisecond, which is coarser than the
  interval between successive calls, so the minted value stops tracking the clock (a `now > last ?
  now : last + 1` mint climbs above it) and any OTHER quantity derived from the same clock — a
  floor at `now - window`, a deadline — silently disagrees with it. Use
  `performance.timeOrigin + performance.now()`: the same wall clock at sub-microsecond resolution.
  Combine the two halves in `bigint` after scaling the small one, never as one float — the sum in
  nanoseconds is past float64's integer precision and would quantise straight back. The trade is
  that `performance.now()` is monotonic and does not absorb wall-clock corrections made after
  process start; for a minted-value clock that is the direction you want.

## Types and members

C# has several ways to attach a name to a value that TypeScript does not. Each has one right
answer, and using a different one per file is how a port becomes inconsistent.

| C# | TypeScript |
|---|---|
| `static class` used as a namespace | module-level exported functions, with the type name folded into the function name where it would otherwise be ambiguous (`AllowedRelationIdentity.Source` -> `allowedRelationSource`). There is no namespace object and no barrel. |
| a record instance property that is really a predicate | a free function (`Relation.IsPermission` -> `isPermission(relation)`) |
| a class whose only public member is one method, or a private constructor plus a static entry point | a module-private class and one exported free function (`Lexer.Tokenize` -> `tokenize`, `Parser.Parse` -> `parse`). Where the C# type also exposes **properties or instance methods** on the object the entry point hands back, export an `interface` for that public surface, keep the class module-private, and have the factory return the interface. Exporting the class instead re-exposes the constructor the C# deliberately made private, and callers start building the type the factory was there to guard. |
| a **static singleton property** on an abstract record | a frozen module constant (`Object.freeze`), **never** a factory function — call sites compare it by reference, and a factory silently breaks every such match |
| a **default interface method** (an interface member with a body) | an exported free function named after the member (`defaultGreaterThan`), which implementations delegate to. Not a base class: the set of implementations is usually open across packages. |
| an **empty record** used as a proto placeholder, or an **empty subclass** existing only as a distinct DI registration key | an interface with a phantom brand (`readonly __trait?: never`) — plus a single frozen instance for the placeholder case. A bare empty interface is structurally satisfied by every object, so the naive port compiles and is silently useless. Name the brand after **its own type** (`readonly __activationMemoOptions?: never`) whenever more than one such type exists in the port: two brands spelled `__trait` unify with each other, which is exactly the mis-wiring the brand was added to catch. |
| an enum with **explicit, proto-mirroring numeric values** | a string-literal union plus an explicit bidirectional wire map (`xToWire` / `xFromWire`, the latter returning `undefined` for unknown values). Never let the wire number ride on declaration order. |
| an enum that is not wire-visible | a string-literal union with no map |
| a **`[Flags]`** enum | stays **numeric** — the string-union row above would break every `(x & Flag) !== 0` call site. Transcribe a combined member (`All = A \| B`) literally rather than re-deriving it as "all the bits". |
| an instance **method** on a record | a free function, name-folded like the predicate row above (`Matches` -> `relationshipsFilterMatches`) |
| a static method **overload set** | two distinctly named functions, folding the distinguishing parameter type into each name |
| an overload distinguished by a parameter **inserted positionally in the middle** | still two named functions, each spelling out its full parameter list in the C#'s order. One function with an optional middle parameter is the trap: every call site that omits it binds its next argument into the inserted slot, which type-checks whenever the neighbouring parameters share a type and silently feeds the wrong value through. |
| an instance property with a **computed body** | a **getter**, never a field snapshot. Porting it as a field is a silent behaviour change, and a TypeScript interface cannot require getter-ness — document and test-pin it. |
| a private `Lazy<T>` field exposed through a property (`private readonly Lazy<T> _x = new(() => Build(...)); public T X => _x.Value;`) | a **memoising getter** over a private field — `get x(): T { this.#x ??= build(...); return this.#x; }`. `LazyThreadSafetyMode.ExecutionAndPublication` maps to nothing on a single-threaded event loop, but the ONCE-ONLY and the PER-INSTANCE halves of `Lazy` both do, and both are load-bearing: hoisting the value into a module-level cache makes two instances of the same data share it, which defeats the reason the field was per-instance (a swapped snapshot drops its graphs for GC instead of accumulating them process-wide). Two smaller differences to state at the site: the C# `Lazy` does not run in the constructor, so a factory that throws leaves construction succeeding and fails only on first read; and `ExecutionAndPublication` CACHES that exception, where a `??=` field recomputes and throws afresh on each later read. This is also the case where the house preference for a `readonly` interface yields to a **class** — an interface cannot hold the memo. |
| a method returning `Task`/`Task<T>` whose argument guards throw **synchronously** (a non-`async` method, or the guards preceding the first `await`) | making the port `async` moves those throws into a **rejected promise**. That is usually right — every caller awaits — but it is a real change in how the failure is observed, so pin it in the test (`await expect(p).rejects.toThrow(...)`, not `expect(fn).toThrow(...)`). Keep the method non-`async` and return the inner promise only where a caller genuinely depends on the synchronous throw. Note that an `async` body still runs synchronously up to its first `await`, so a side effect the C# performed before returning the task — registering a lookup, incrementing a counter — still happens before the caller's next statement. |
| a default **parameter** value | an absent optional member plus a named resolver using `??` (so an explicit `0`/`false` survives), not a default in the type |
| `bool TryX(string? input, out T result)` | a function returning `T \| undefined`. Read what the C# does on each path FIRST: the common shape returns `false` for absent/empty input and **throws** for malformed input, and those two are not the same answer. Collapsing the throw into `undefined` turns a corrupted wire token into "start from the beginning", which silently restarts a paged read instead of rejecting it. |
| a value-tuple return `(ulong Count, bool Flag)` | a named `readonly interface` — a labelled TS tuple's labels are unchecked |
| a **mutable public auto-property** on an otherwise-immutable object (`IDispatcher Dispatcher { get; set; }` defaulting to `this`) | a **class** with a public mutable field. This is the case where the prefer-`interface`-and-`readonly` rule points the wrong way: the property is a seam that tests and composition roots reassign **on a live object**, so a frozen literal, a `readonly` member or a constructor-only option makes the seam unusable and forces the port to invent a different wiring than the source's. |
| a nested sealed record hierarchy whose nested types **re-declare** the base's positional members | one discriminated union whose members each carry those members in full — the re-declaration is not extra state, so do not model it as a base object plus a nested one. An abstract-typed `node with { Caveat = c }`, which C# dispatches to the concrete clone, is then just `{ ...node, caveat: c }`; that is only correct while the discriminant is a plain data field, so never compute `kind` in a getter or method. A spread copies own enumerable properties only, so a `kind` living on a getter or a prototype is silently dropped by `{ ...node }` and the clone stops matching any arm of the switch it was cloned to feed — the declared type is still the union, so nothing warns and the loss surfaces at runtime as an unexplained default-arm hit. |
| `params T[]` | a rest parameter `...xs: readonly T[]`. Preserve the ARGUMENT ORDER at every call site: where the callee returns or throws on the first failure, the order decides which message the client sees. |
| a `readonly record struct` used only as a `params` element or a plain argument — never as a dictionary/set key | a `readonly` `interface`. Value equality is what a record struct buys and what a `Map`/`Set` key would need; when nothing uses it, an interface is the faithful port and a class only adds a constructor the C# did not have. |
| a private field `_x` | `#x`, and `private readonly x` **only** where a constructor parameter property is the natural spelling. Pick one per class rather than mixing: `#x` is genuinely inaccessible at runtime while `private` is erased, so a half-and-half class leaves it unclear which members a test is allowed to reach and which the port is free to rename. |

Two TypeScript settings change every signature mechanically. `exactOptionalPropertyTypes` means
a C# `?` member becomes `?: T | undefined`, not `?: T`. `noUncheckedIndexedAccess` means every
indexing expression already yields `T | undefined`, which is usually what the C# meant anyway.

## Exceptions

| C# | TypeScript |
|---|---|
| `ArgumentException` / `ArgumentNullException` | a project `InvalidArgumentError`. Keep the guard even where the parameter's TypeScript type is non-optional — the caller may be untyped. |
| `FormatException` | a project `FormatError` |
| `NotSupportedException` | a project `NotSupportedError`. Distinct from `InvalidArgumentError`: the argument is well-formed, the OPERATION is outside the seam's contract, and the two map to different gRPC codes. Where the C# throws it from an `internal`/narrow seam and no shared class exists yet, declare it beside the throw rather than inventing a package-wide vocabulary the C# does not have. |
| `InvalidOperationException` | a project error class, named for the invariant it protects |
| `catch (Ex) when (cond)` | an exception filter. Read `cond` first: a constant-true filter is a plain `catch`, not a condition to port. |
| a filter listing **BCL exception types** (`when (ex is CelException or InvalidOperationException or ArgumentException)`) | the port has usually collapsed those into fewer classes, so match the **mapped project errors plus a plain `Error`** — the ported layer beneath often throws a plain `Error` where .NET threw `InvalidOperationException` — and **rethrow** `TypeError`, `RangeError`, `EvalError` and any non-`Error` throw. Catching everything converts a programming fault into the source's user-facing error and hides the bug; catching only the mapped classes misses the path the port itself now throws on, so the C#'s tolerant branch never runs. |
| `new Ex(message, inner)` | `super(message, { cause: inner })` — the ES2022 option. Type `inner` as `unknown`: a caught binding need not be an `Error`. |
| the Orleans transport family — `SiloUnavailableException`, `OrleansMessageRejectionException`, `TimeoutException`, and `OrleansException` as the catch-all base | no one-to-one names. `RejectionError` (its `kind` names the refusal, so both silo-unavailable and message-rejected land here), `GrainCallTimeoutError`, and `GrainCallError` as the general dispatch/execution failure. All three extend **`ThreshRuntimeError`**, the `OrleansException` analogue, so a C# `catch (OrleansException)` ports as the single predicate `isThreshRuntimeError(error)` rather than an open-coded `instanceof` list a later error class silently falls through. The base sits ABOVE the three: a `RejectionError` is still not an `instanceof GrainCallError`, so narrowing to a leaf keeps meaning exactly what it meant. Never widen the arm to a bare `Error`: `TypeError`/`RangeError` must stay programming faults, not retriable transport failures. The storage and transaction error classes deliberately sit OUTSIDE this base — a lost etag or an aborted transaction is an outcome the caller acts on, not a transport failure to retry — so keep listing those explicitly. |
| a filter around a **ported parse** — `when (ex is FormatException or OverflowException or ArgumentException)` | match what the PORTED parser actually throws, which is often a JS built-in: a hand-rolled `NumberStyles` check typically throws `SyntaxError` for the shape and `RangeError` for the out-of-range value. This is the one place the "rethrow `TypeError`, `RangeError`" rule above is **wrong** — there `RangeError` is a programming fault, here it is the source's `OverflowException` and must be caught. Decide it from the throw sites of the specific parse being called, and say so at the catch. |
| `OperationCanceledException` (and `TaskCanceledException`, which derives from it) | `isCancellationError(error)` (`@thresh/core/errors`) — one predicate over the whole **abort family**: `GrainCallAbortedError`, `GrainTaskCanceledError` (both extend `ThreshCancellationError`, the `OperationCanceledException` analogue) and a DOM `AbortError`, which no class base can reach. `ThreshCancellationError` is deliberately NOT a `ThreshRuntimeError`, mirroring `OperationCanceledException` not being an `OrleansException`: a cancellation is the caller getting what it asked for, and classifying it as a call failure turns a deliberate abort into a retriable availability error. |
| `RpcException(new Status(StatusCode.X, detail))` at a gRPC service boundary | a project `RpcError extends Error` carrying a numeric `code` (from `@grpc/grpc-js`'s `status`) and a `details` string, plus an empty `Metadata`. `@grpc/grpc-js` has no `RpcException`: what it consumes is a `ServiceError`, which is an `Error` with exactly those three members, so one class serves both the C#'s `ex.StatusCode`/`ex.Status.Detail` reading and the transport's, and can be handed to `sendUnaryData`/`stream.destroy` unchanged. Declare it ONCE per port — every service file throws the same class — and set `this.name`. Do not reach for a status-per-error-class hierarchy: the C# picks the code at the throw site and those choices are wire-visible. |

**Any exception type a caller catches BY TYPE across a grain call wants a surrogate.** Orleans
resolves the wire's type name through its `TypeConverter` and rebuilds the exception as itself;
Thresh's codec has no type resolution for application classes, so `registerSurrogate` is what
supplies it. Without one, the error still arrives — as an
`UnavailableExceptionFallbackException` (`@thresh/core/errors`), the port of upstream's
[`UnavailableExceptionFallbackException`](https://github.com/dotnet/orleans/blob/main/src/Orleans.Serialization/ISerializableSerializer/UnavailableExceptionFallbackException.cs),
carrying the sender's type name on both `name` and `errorType`, the message, and the original's own
enumerable properties (under `properties`, and copied onto the instance so `error.limit` reads).
That is a floor, not a substitute: `instanceof MyDomainError` still misses, and a `catch (MyEx)`
transliterated as an `instanceof` arm still runs the wrong branch. Discriminating on `name` is the
fallback for a type that cannot take the registration. Thresh's OWN error classes need nothing —
the codec rebuilds them by name, so `isThreshRuntimeError` and `isCancellationError` keep firing
across a silo boundary.

When registering an exception surrogate, encode **only** the carried data where the constructor
re-derives its message from it; encode the message only when it is the type's sole distinguishing
state, or when something downstream parses that message back into structure — a byte-exact message
contract makes the message carried data, not a derivable convenience.

An exception belonging to a layer that must not take a Thresh dependency, but that still crosses a
grain boundary, gets its `registerSurrogate` call in the **grain layer instead**, in a module whose
only contract is the side effect of importing it. Write the `test` predicate against the concrete
class, never a shared base: later registrations are checked first, so a base-class predicate
swallows every sibling into one tag.

Every ported exception class needs `Object.setPrototypeOf(this, new.target.prototype)` in its
constructor for `instanceof` to survive downlevelling, and an explicit `this.name`. This is
mechanical and applies to all of them.

## Parsing, formatting and encoding

The .NET BCL is stricter than its JavaScript counterpart almost everywhere, and the strictness
is frequently load-bearing: an error path that exists only because .NET throws becomes
unreachable when the JS equivalent quietly succeeds. These are the ones that have actually bitten.

| C# | The trap |
|---|---|
| `Convert.FromBase64String` | throws on malformed input; `Buffer.from(s, "base64")` **never throws** — it skips invalid characters and truncates. Validate the alphabet, padding and length first. .NET's decoder ignores exactly space, tab, CR and LF — not `\v`, not `\f`. |
| `Convert.ToBase64String` | **standard** base64 — the `+`/`/` alphabet with `=` padding — never the URL-safe variant, whatever a neighbouring doc comment claims. When a comment and the call disagree about a token's encoding, the CODE is the contract: the token is already in clients' hands. Port the code and fix the comment. |
| `int.TryParse(s, out var x)` / `long.TryParse` with the **default** `NumberStyles` | `NumberStyles.Integer` = leading sign + leading/trailing whitespace allowed, everything else rejected. That is a DIFFERENT parse from the same call passing `NumberStyles.None`, and a codebase usually has both. Write one regex per style and keep them separate — unifying them either starts accepting `" +12 "` where the source rejected it, or rejects it where the source accepted it, and both are wire-visible in a cursor. |
| `long.Parse(s, InvariantCulture)` | throws on hex, exponent notation, empty input and out-of-range values; `BigInt(s)` accepts `0x2a` and is unbounded. Regex-validate the `NumberStyles.Integer` shape, then `BigInt`, then range-check against int64. |
| `DateTimeOffset` | 100ns tick resolution, seven fractional digits, and .NET Core rounds fractions **half away from zero** rather than truncating. `AddTicks` with negative instants needs an explicit floor adjustment, because C# `long` division and `BigInt` division both truncate toward zero. |
| `string.Trim()` / `char.IsWhiteSpace` | the .NET and JS whitespace sets genuinely differ (U+0085, U+FEFF). If the difference is wire-visible, hand-roll the .NET set. |
| `char.IsLetter` / `IsLetterOrDigit` / `IsDigit` | Unicode-aware; JS `\d` and `[a-z]` are not. Use `\p{L}` / `[\p{L}\p{Nd}]` / `\p{Nd}` with the `u` flag. A C# `char` is a UTF-16 code unit, so index by code unit, not code point. |
| `\w` and `\b` **inside a ported regex** | the same trap one level up: .NET's `\w` and `\b` are Unicode-aware, JS's are ASCII-only and stay ASCII-only under the `u` flag. Spell `\w` out as `[\p{L}\p{M}\p{Nd}\p{Pc}]` and rebuild `\b` as explicit lookarounds over that class. Left alone, an identifier or word-boundary match quietly rejects every non-ASCII name the C# accepted, and the failure looks like bad input rather than a bad port. |
| `Regex.Escape(s)` | no JS counterpart — hand-roll it, and escape exactly what .NET escapes. The usual `[.*+?^${}()\|[\]\\]` list omits **whitespace and `#`**, which .NET escapes because they are significant under `RegexOptions.IgnorePatternWhitespace`; a pattern built from user text then matches differently on either side. |
| `IPAddress.TryParse` / `IPAddress.Parse` | no JS counterpart, so the parse is hand-rolled — and hand-roll it **strictly** rather than reproducing .NET. .NET accepts leading zeros (read as octal), `1.2.3` shorthand, bare integers and hex forms, none of which a wire format means, and all of which turn a rejected address into a silently different one. Write the strict parse (dotted quad with no leading zeros; RFC 4291 IPv6 with one `::` and an optional trailing embedded IPv4), record it as a deliberate divergence, and pin it with tests. |
| a non-multiline `$` in a regex | .NET's `$` also matches immediately before a single trailing `\n`; JS's does not. Anchored expressions need `\n?$`. |
| `JsonSerializer.Serialize` | `JavaScriptEncoder.Default` escapes more than `JSON.stringify` does: `&`, `'`, `+`, `<`, `>`, backtick, **every** control character, DEL, and **every** non-ASCII UTF-16 code unit — as `\uXXXX` with **uppercase** hex, astral characters as two escaped surrogate halves. If the output is wire-visible, hand-roll it. |
| `JsonSerializer.Deserialize<T>` | returns `null` for the `null` literal, matches property names **case-sensitively** by default, skips unknown members silently, and **throws** on a type mismatch. `JSON.parse` shares none of that. |
| `Uri.EscapeDataString` | **not** `encodeURIComponent`. .NET escapes everything outside the RFC 3986 unreserved set `A-Za-z0-9-._~`; `encodeURIComponent` additionally leaves `!'()*` alone. Hand-roll it: percent-encode the UTF-8 bytes, uppercase hex. Where the result is a grain key the difference is identity-visible, not cosmetic. |
| `Uri.UnescapeDataString` | **never throws**; `decodeURIComponent` throws `URIError` on a malformed `%` sequence. .NET leaves a bad escape — and a percent-escaped byte run that is not valid UTF-8 — as the literal source text with its original case, decoding the valid escapes around it. Hand-roll that too, or a parse whose only documented failure is a *shape* check starts throwing an unmapped runtime error instead. |
| `SHA256.HashData` + `Convert.ToHexStringLower` | `createHash("sha256").update(b).digest("hex")` from `node:crypto` — WebCrypto's `subtle.digest` is async and unusable from a synchronous method |
| `Guid.NewGuid().ToString("n")` | `crypto.randomUUID().replace(/-/g, "")` — 32 lowercase hex, no dashes |

## CEL, and libraries that differ in kind

`@bufbuild/cel` is not a differently-spelled `Cel`: it differs in **kind**. The .NET package
signals failure by throwing typed exceptions (`CelUndeclaredReferenceException`,
`CelNoSuchOverloadException`); the TypeScript one **returns** `CelError` and `CelUnknown` from
`env.run` as ordinary values. Every `try`/`catch` in the source is control flow with no landing
site in the port. The rows are written for CEL because that is where it bit, but each is really
about a dependency whose shape — not whose API — differs from the .NET one.

| .NET | TypeScript |
|---|---|
| a library that **throws** typed exceptions | one that **returns** errors as values. Invert the control flow: the `try` body becomes a straight call and each `catch` arm becomes a predicate over the returned value. A transliterated `catch` compiles and never fires, so the source's fallback (missing context -> caveated, say) is simply lost and the error value flows onward as if it were a result. |
| a `catch` filter that **classifies** the exception | the same classification, made from the evidence the value carries: its class, its message, and any nested errors. Errors merge, so walk the nested chain (`CelError.additional`) recursively rather than inspecting only the outermost value. Match message shapes the library actually produces — a predicate copied from the C#'s exception-type list matches nothing at all, and the port then treats every failure as the default arm. |
| `catch (LibEx) { throw; }` — a rethrow | there is no exception to rethrow, so synthesise one: `throw new Error(message, { cause: value })`, keeping the C#'s message text and carrying the error value in `cause`. Do **not** reach for a project error class merely to have something to throw — those carry gRPC status codes, and borrowing one turns an internal fault into a client error that makes `zed` retry. |
| a library object the C# holds as a **field**, passing per-call state as a parameter (`CelEnvironment.Program(expr, vars)`) | check the JS object's lifetime before copying the field across. Where the JS equivalent keeps that state **on the instance** (`CelEnv.set` mutates `data`), build it **per call**: a shared instance leaks one request's variables into the next as ambient context, which no single-request test can see. Call-scoped in .NET against instance-scoped in JS is the general shape, and it always resolves to constructing per call. |
| a library with its own **numeric tower** | port the carriers once, at the boundary, and write the table down (CEL: `int` -> `bigint`, `uint` -> `CelUint`, `double` -> `number`). Keep the wrapper where it encodes a property the primitive loses — `CelUint` is not a bare `bigint` because unsignedness is observable above 2^63. Cross-width equality that C# got free from one `object` comparison must be re-implemented, since `1n !== 1`: without it a comparison between an int-typed and a double-typed value silently reports "not equal" and the containing predicate goes false. |
| converting a string into one of those carriers | the .NET conversion range- and shape-checked for free. `BigInt(s)` accepts `0x2a` and is unbounded and `Number(s)` accepts `Infinity`, so regex-validate the `NumberStyles` shape first, convert, then range-check against the carrier's width — and expect a new failure mode the C# type system made unreachable. |

## Collections and LINQ

| C# | TypeScript |
|---|---|
| `IEnumerable<T>` | `Iterable<T>` or `readonly T[]` |
| `IAsyncEnumerable<T>` | `AsyncIterable<T>` — an `async function*` generator. Decide re-enumerability from the **producer**, not the type: a method that reopens its source on each `await foreach` (a fresh query, a re-read) may be materialised into an array, but one that walks a cursor, a stream or an open connection is genuinely single-pass and must stay a generator. Materialising a single-pass traversal buffers an unbounded result set and defeats the early exit its caller relies on; making a re-enumerable one a generator silently yields nothing on the second pass. |
| a **private** async iterator method (`private async IAsyncEnumerable<T> LookupRec(...)`) | a private generator method — `async *#lookupRec(...)` — which is exactly what TypeScript supports and is the right shape. Do not lift it to a module-level generator taking the instance as a parameter: that widens a private helper into a module surface and loses `this` for no gain. |
| `await foreach (var x in xs)` | `for await (const x of xs)` |
| `IReadOnlyList<T>` / `IReadOnlyDictionary<K,V>` | `readonly T[]` / `ReadonlyMap<K,V>` |
| `ImmutableArray<T>` / `ImmutableDictionary<K,V>` | `readonly T[]` / `ReadonlyMap<K,V>`, copied on write |
| an `ImmutableHashSet<T>` / `ImmutableDictionary<K,V>` threaded through a **recursion** (`Walk(child, visited.Add(k))`) | copy at the call: `walk(child, new Set(visited).add(k))`, never `visited.add(k)` on a set shared with the caller. The C# expression returns a *new* set and leaves the caller's untouched, so each sibling branch sees only its own ancestors' keys. A shared mutable `Set` compiles and passes every single-path test, then prunes a live sibling branch because an earlier sibling already visited the key — a missing result, not a crash. |
| `.Select` / `.Where` / `.Any` / `.All` | `.map` / `.filter` / `.some` / `.every` |
| `.FirstOrDefault()` | `arr[0] ?? undefined` — with `noUncheckedIndexedAccess`, indexing already yields `T \| undefined` |
| `.FirstOrDefault(predicate)` over a sequence of **structs** | `arr.find(p)` returns `undefined`, but .NET returns a **zero-valued instance** with every member defaulted — not null, and freely dereferenced. A mechanical port that then reads a member throws on the not-found path where the C# read a default. Collapse the C#'s `FirstOrDefault` + separate `Any`/null-check pair into one `find` plus an explicit `undefined` check, and say at the site that the structure differs because the C#'s default value has no TypeScript counterpart. |
| `.SingleOrDefault()` | explicit length check; there is no built-in |
| `.OrderBy(k)` | `[...xs].sort(byKey)` — `sort` mutates, so copy first. Note .NET `List<T>.Sort` is an **unstable** introsort while `Array.prototype.sort` is stable: whenever the comparator has ties, the two disagree. |
| `xs.Sort((a, b) => a.Rev.CompareTo(b.Rev))` where `Rev` ported to **`bigint`** | an explicit comparator returning **-1/0/1**. `a - b` returns a `bigint`, which `sort` rejects at runtime, and a comparator-free `sort` compares the values as STRINGS — so `10n` sorts before `9n` and a revision-ordered list silently comes back scrambled. |
| `IEnumerable<T>` built with `yield return` | a returned **array**, not a `function*`. A C# `IEnumerable` is re-enumerable; a TS generator is single-pass and silently yields nothing the second time. Use `function*` only where the C# was itself single-pass. |
| `List<T>.GetRange(i, n)` | `xs.slice(i, i + n)` — but `GetRange` throws on a negative count while `slice` counts from the end. See the unsigned-range note under Serialization. |
| `.GroupBy(k)` | `Map.groupBy(xs, k)` |
| `.ToDictionary(k, v)` | `new Map(xs.map((x) => [k(x), v(x)]))` — but `ToDictionary`/`ToImmutableDictionary` **throw** on a duplicate key while `new Map(...)` and `map.set` silently overwrite. Where the C# relied on that throw, add an explicit check. |
| `Dictionary<K,V>` with a **struct/record key** | `Map` keys compare by reference — key by a canonical string, or the lookup silently misses. This is the single most common porting bug. |
| `Dictionary<K,V>` iteration order | .NET's is unspecified and disturbed by removals; a JS `Map` is insertion-ordered and stable. Usually benign — but it is often *why* the C# sorts defensively, so never "simplify away" such a sort. Ask per sort whether its order is **observable** — does it feed a cursor, a first-match short-circuit, a truncated page, a returned sequence? Where it is, keep the sort verbatim even though the `Map` is already ordered, because insertion order and the C#'s sort key are not the same order and the difference is a wire-visible change. Where nothing downstream can see it the sort is merely defensive, and it still costs nothing to keep. |
| `Dictionary<K, V?>` where null is meaningful | `Map<K, V | undefined>`: keep `has` and a defined `get` distinct, and never collapse the read with `??`. |
| `record with { ... }` | object spread `{ ...state, x }` — a fresh object, with untouched members still shared by reference and never mutated. A `static readonly` struct instance that call sites copy with `with` becomes a frozen module constant (see the static-singleton row) **plus a spread at every site**: the naive port assigns into the shared constant instead, and without `Object.freeze` that write succeeds and corrupts every later reader, far from the line that made it. |
| `new HashSet<string>(StringComparer.Ordinal)` | a plain `Set<string>`. JS string equality IS ordinal, so the comparer is already the default — do NOT add case folding or `normalize()`; a key differing only in case or in Unicode composition is a different key in both languages, and where the key is echoed back in an error message that difference is wire-visible. |
| `HashSet<T>` of records | same problem: `Set<string>` over a canonical key — but only where the set answers membership questions. Where the C# projects the **elements** back out (returns them, enumerates them into a result), a `Set<string>` has thrown away the very values the caller needs and the port ends up re-parsing keys back into objects: use a `Map<canonicalKey, T>` and hand back `[...m.values()]`. |
| `Queue<T>` with `Enqueue` / `Dequeue` | `T[]` with `push` and **`shift`**. `pop` is LIFO: it turns a breadth-first worklist into a depth-first one, which still terminates and still visits every node, so a test asserting the *set* of results stays green while anything order-sensitive — a first match, a cursor, a truncated page — changes. `Stack<T>` with `Pop` is the one that maps to `pop`. |
| `SortedDictionary` / `SortedSet` | no equivalent; keep a sorted array + binary search, or sort on read. `Array.prototype.sort` with **no comparator** compares by UTF-16 code unit, which is exactly `StringComparer.Ordinal`, so a bare `.sort()` over strings reproduces a .NET ordinal `SortedSet` verbatim — this is what makes a ported cursor ordering correct. Supply a comparator only for non-strings or for a non-ordinal .NET comparer, and never `localeCompare`. |

**`IAsyncEnumerable` across a grain boundary has no equivalent.** Orleans can return one from a
grain method; Thresh cannot. If the source does this, the port must convert that call into a
paged/cursor protocol. (Streaming *within* a process is fine and maps directly.)

## Concurrency and cancellation

| Orleans | Thresh |
|---|---|
| `CancellationToken` | `AbortSignal`; the ambient one is `ctx.runtime.getCancellationSignal()` |
| `token.ThrowIfCancellationRequested()` | `signal?.throwIfAborted()` — the standard DOM method; Thresh adds no wrapper |
| `CancellationTokenSource.CreateLinkedTokenSource` | `combineSignals(...)` (`@thresh/core/abort`) |
| `task.WaitAsync(token)` | `raceSignal(promise, signal)` (`@thresh/core/abort`) |
| `Task.Delay(d, ct)` (a **bounded** delay) | a `setTimeout` promise wrapped in `raceSignal(sleep, signal)`, with the timer cleared in a `finally`. Thresh ships no `delay` helper, so write the three lines locally. A bare `await new Promise(r => setTimeout(r, d))` is **not** a translation: `Task.Delay` observes the token and throws, so dropping the race leaves one unkillable window per iteration — across a retry loop that is the whole retry budget spent after cancellation. |
| `Task.WhenAny(work, Task.Delay(Infinite, ct))` | `raceAbort(promise, signal)` (`@thresh/core/abort`), which resolves to the `ABORTED` sentinel instead of rejecting — **cancellation as a clean exit**. Porting this with `raceSignal` turns a `yield break` into a thrown `GrainCallAbortedError`. |
| `Task.FromResult(x)` | `Promise.resolve(x)` from a **non**-`async` method |
| `Task<T?>` returning null / an optional parameter passed as `null` | `Promise<T \| undefined>` returning `undefined`, and an argument passed as `undefined`. Both survive a grain call as `undefined` — the codec tags a top-level `undefined` and one in a positional slot (array element, `Map` key/value, `Set` member), which is how a call's arguments travel. An `undefined` **member of an object** still travels as an omitted key, which reads back identically. So port C# `null` to `undefined` throughout and keep `=== undefined` guards; do not defensively test for `null` on a grain result. |
| `TaskCompletionSource<T>` | a `{ promise, resolve }` pair; `RunContinuationsAsynchronously` is a no-op in JS |
| `CancellationToken` on a **plain interface** (no grain, no ambient source) | a trailing `signal?: AbortSignal | undefined` parameter, kept in the C#'s positional slot |
| `CancellationToken` on a **grain interface** (Orleans' native grain-call cancellation) | the same trailing `signal?: AbortSignal | undefined` parameter. The grain factory converts a signal argument into the cancellation shape the wire carries and the callee is handed an `AbortSignal` back, so the callee's view does not depend on placement, and an abort after the call is sent reaches the activation. A signal **nested inside an argument** — the shape a ported request record takes when it carries the `CancellationToken` itself — is converted and unwrapped the same way, at any depth reachable through arrays, plain objects, `Map` values and `Set` members. It is **not** reached through a class instance: keep the signal in a plain record (or in its own parameter) if the record's type is a class. |
| `IAsyncEnumerable<T>.WithCancellation(token)` | no equivalent — `AsyncIterable` has no signal channel. Check the signal in the loop body, and have the producer take it too. |
| `Task.WhenAll(xs)` | `Promise.all(xs)` |
| `Task.WhenAny(xs)` | `Promise.race(xs)` |
| `.ConfigureAwait(false)` | delete it |
| `SemaphoreSlim` | `AsyncSerialExecutor` (`@thresh/core/async-serial-executor`) for a semaphore of ONE **whose critical section is one callable unit**, but first check whether the grain's single-turn guarantee already makes it unnecessary. Where the source HOLDS the permit across a section it does not own as a callback — `await sem.WaitAsync()` at the top of a method and `sem.Release()` in a `finally` many statements later, especially when the release is conditional — `AsyncSerialExecutor` does not translate: it runs a queued callback to completion rather than handing out a permit. Write a small promise-chain mutex (`wait()` awaits the previous holder's promise and takes the next one; `release()` resolves it; release in a `finally`) beside the call site, and keep it NON-REENTRANT so the source's deadlock properties are unchanged. For an initial count ABOVE one — `new SemaphoreSlim(Math.Max(1, maxConcurrency))` around a `Task.WhenAll`, i.e. a BOUNDED fan-out — Thresh has no equivalent: `AsyncSerialExecutor` is strictly serial. Write a small local counting semaphore (FIFO hand-off of a released permit, `wait()` / `release()`, `release` in a `finally`) beside the call site. `Promise.all` over the raw list is NOT a translation: it starts every task at once, so the fan-out the source deliberately bounded becomes an unbounded burst, which no unit test notices and the mesh feels immediately. |
| `lock (x)` | delete it — a grain activation is single-threaded per turn. Keep a lock **only** if the C# guarded state shared *between* grains, which is itself a smell the port should remove. |
| `RequestContext.Set/Get` | `ctx.runtime.setRequestContext(key, value)` / `getRequestContext(key)` inside a grain, or the static `RequestContext` from `@thresh/core/request-context` (`get`/`set`/`remove`/`clear`/`keys`) from anywhere, including non-grain code — so a C# static context helper ports shape-for-shape, and `RequestContext.Clear()` has an exact counterpart in tests. Two differences. **(1) Values are `string` only**, where Orleans takes any serializable object: anything richer must be encoded at the set and decoded at the get — and a **decode failure must be as loud as a missing key**, or a corrupted value silently becomes a default. **(2) `set` MUTATES the ambient store in place**, where Orleans' is copy-on-write (a fresh dictionary per set). So a value set inside an awaited callee **does leak back UP to the caller** after the await, which Orleans guaranteed it would not. Sibling isolation still holds only by discipline: every call site must set the values immediately before its own outgoing call and never rely on what a previous sibling left behind. Port any C# remark promising the Orleans scoping guarantee as a pinned test of the divergence, not as an assumption. |
| `GrainCancellationToken` | `GrainCancellationToken`, same shape |

Note the direction of the danger: C# `async` methods can interleave at every `await` and Orleans
grains are non-reentrant by default; TypeScript is the same. A ported grain that was
`[Reentrant]` in C# and is not marked `reentrant` in Thresh will deadlock on self-calls, and one
that was non-reentrant but is marked reentrant will corrupt state silently. Carry the attribute
across deliberately.

## Placement

| Orleans | Thresh |
|---|---|
| `[RandomPlacement]` (default) | default, or `{ placement: "random" }` |
| `[PreferLocalPlacement]` | `{ placement: "preferLocal" }` |
| `[ActivationCountBasedPlacement]` | `{ placement: "activationCount" }` |
| `[SiloRoleBasedPlacement]` | `{ placement: "siloRole", role }` |
| `[ResourceOptimizedPlacement]` | `{ placement: "resourceOptimized" }` |
| `IPlacementFilterDirector` | a director registered via `addPlacementFilter(name, director)`, selected with a `"custom"` filter descriptor |
| `IPlacementDirector` (a custom *strategy*) | a `PlacementStrategy` registered via the silo builder's `addPlacementStrategy(name, strategy)`, selected with `{ placement: "custom", strategy: name }`. Stateless-worker placement still wins over it, as in Orleans. |
| `IPlacementDirector.GetPlacementHint` | the placement-hint helper — hints ride in the request context and every strategy must honour them |
| `PlacementTarget.GrainIdentity.Key` | `PlacementContext.grainId` — the dispatcher passes the whole `GrainId` (type and key) to every filter and strategy, so a key-aware director can hash the key |
| `PlacementStrategy` marker class + `PlacementAttribute` subclass | nothing — a director *is* a `PlacementStrategy`; what survives the pair is the registry NAME string |
| `Array.Sort(SiloAddress[])` | `[...candidates].sort(SiloAddress.compare)` — a total order over `podName`, then `podUid`, then `endpoint`, mirroring Orleans' component-by-component `SiloAddress.CompareTo`. It is ORDINAL, never `localeCompare`, and returns 0 exactly when `equals` is true, so every silo derives the same order from the same membership view. Do not invent a second ordering (a `toString()` sort, say) in another strategy: two strategies ordering candidates differently is a placement split-brain on the same grain key |

Two notes a mechanical port trips on. Orleans' hint branch (`GetPlacementHint` first, strategy
second) is already performed by the dispatcher above the strategy, so re-implementing it inside
`choose` is dead code. And `Array.prototype.sort` sorts IN PLACE where `Array.Sort` follows a
`Clone()`: copy the candidate array before sorting, or the caller's list is reordered.

## Hosting and DI

| Orleans | Thresh |
|---|---|
| `Host.CreateApplicationBuilder().UseOrleans(b => ...)` | `createSilo({ ... })` |
| `builder.UseLocalhostClustering()` | static membership in the silo options |
| `builder.AddMemoryGrainStorage("name")` | `useMemoryStorage({ name })` |
| `services.AddSingleton<IFoo, Foo>()` | a module-level value, or a factory passed into `createSilo`. Thresh has no DI container; constructor injection becomes explicit wiring. |
| `[FromKeyedServices("name")] IGrainStorage` | resolve the named storage from the silo's storage registry and pass it in |
| `cluster.Services.GetRequiredKeyedService<IGrainStorage>("name")` in a TEST, to read or seed raw storage rows | there is nothing to resolve it back out of. Construct the provider in the TEST, hand the SAME instance to the silo (`builder.addStorage("name", storage)`) and keep the handle for `storage.read`/`storage.write`. Never build a second provider over the same database for the assertions: it grades rows nothing under test wrote, and a migration or flush bug that writes the right key with wrong content still passes. |
| `IOptions<TOptions>` | a plain options object with defaults applied at the call site |
| `ILogger<T>` | the logger from the grain context, or the module logger |
| `services.AddHostedService<T>()` (a service whose only job is to start something at silo boot) | `builder.addStartupTask(async (grains) => ...)`. The task is handed a `GrainFactoryAccess`, which also exposes `createObjectReference`, so anything that must mint an observer reference at boot belongs here — and a silo that does mint one should also call `builder.requireObserverHosting()`, which fails the build on a transport that cannot back the seam instead of at the first observer call (see `docs/deviations.md`). |
| a singleton that takes `IGrainFactory` in its constructor | there is no grain factory at wiring time. Bind the one a startup task is handed to a closure variable and make every dependent service LAZY (built on first access, memoized). This is not a workaround: it is the same deferral the container performed. |
| grain **constructor** injection (`class FooGrain(IBar bar) : Grain`) | `builder.useGrainActivator({ createInstance })` plus an explicit options-bag interface per grain. The bag may be a REQUIRED constructor parameter: a registration takes `GrainClass` (`abstract new (...args: never[]) => Grain`), so `registerGrain`/`registerGrains` type-check without a cast, and `createInstance` is handed the class itself — compare it with `ctor === FooGrain`, no cast either. Fall through to `constructGrain(ctor)` (`@thresh/runtime/construct-grain`, the default `new ctor()`) for grain types the activator does not know, the management grain among them. Registration does not check arity, so a required bag whose grain the activator forgets fails inside the constructor at first activation, not at build. |
| `services.TryAddSingleton<T>()` — first registration wins | nothing gives you "is it already registered?". Record the earlier explicit registration yourself (a module-level `WeakMap` keyed by the `SiloBuilder`) and read it back before applying a default. Porting TryAdd as a plain assignment silently reverts a deployment's earlier opt-in with no error, which is exactly what the C# comment on such a call is usually warning about. |
| `AddPlacementDirector<TStrategy, TDirector>()` | `builder.addPlacementStrategy(name, director)` — one call, last-wins. Registering the Orleans pair twice was harmless; registering the strategy twice REPLACES it, so pass the effective options into the director you register last. |
| `GrainCollectionOptions.ClassSpecificCollectionAge[typeof(TGrain).FullName] = age` | `createSilo({ classSpecificCollectionAgeSeconds: { [grainType]: age } })` — a per-SILO map, resolved at the catalog exactly as Orleans resolves it at `GrainTypeSharedContext`, so two silos in one process may hold different ages for the same grain class. Precedence is Orleans': the grain's own `@grain({ collectionAgeSeconds })` FIRST, then the silo map, then the silo's `collectionAgeSeconds` default — `GrainTypeSharedContext.GetCollectionAgeLimit` reads the class's `[CollectionAgeLimit]` and returns immediately, so the map never applies to a type that declares its own age. A grain type that wants per-deployment control therefore declares none. Key by the REGISTERED GRAIN TYPE (`getGrainMetadata(Ctor).grainType`), never `class.name`, which a bundler minifies. Rewriting the decorator with `setGrainOptions` still works and is still process-wide — reach for it only when the age genuinely belongs to the class rather than to the deployment. |
| `services.AddSingleton<PlacementStrategy, TStrategy>()` — replacing the cluster's DEFAULT placement strategy | `createSilo({ defaultPlacementStrategy: new TStrategy() })` (and the same key on `TestCluster.start`). It applies only to grain types that declare no `placement` of their own; an explicit per-class `placement` — `"random"` included — and `stateless: true` both still win, as they do through Orleans' `PlacementStrategyResolver`. Unset, the default stays `RandomPlacement`, matching `DefaultSiloServices`. |
| an ASP.NET Core gRPC service method's `ServerCallContext` parameter | a trailing `signal?: AbortSignal \| undefined`. Read the C# body first: what it almost always touches is `context.CancellationToken` alone, and porting the whole context means porting a type nothing else needs. If it reads headers, deadlines or `Status`, add those as explicit parameters rather than reconstructing the context. |
| `IServerStreamWriter<T>` (server-streaming) and `IAsyncStreamReader<T>` (client-streaming) | a PORT-LOCAL seam: a one-method `ServerStreamWriter<T>` (`write(message): Promise<void>`) and a plain `AsyncIterable<T>`, with `@grpc/grpc-js`'s `ServerWritableStream` / `ServerReadableStream` adapted onto them in the host wiring. `grpc-js` has no counterpart interface — it hands the service a Node duplex whose `write` returns a boolean and whose backpressure arrives as a `drain` event — so binding a service body to it drags the transport into the service and makes the C# suites' fake writers/readers (`CollectingStreamWriter`, `FakeStreamReader`, a writer that cancels mid-write) unportable. Awaiting `drain` belongs in the ADAPTER; a service body only ever awaits `write`. |
| a file of **top-level statements** (`Program.cs`) | an exported `main()` plus a direct-execution guard (`if (process.argv[1] !== undefined && fileURLToPath(import.meta.url) === process.argv[1]) await main();`). A module whose body starts a host cannot be imported by a smoke script, or by a test that only wants to read a constant out of it — and a host started from a test or from CI orphans and runs forever. |
| `IConfiguration` / `configuration["A:B"]` | a one-method `{ get(key): string \| undefined }` over a record, reproducing the two .NET key rules the deployment actually depends on: comparison is CASE-INSENSITIVE, and `__` in an environment variable maps onto the `:` section separator (`ConnectionStrings__OrleansStorage`). Port those two rules; port nothing else about `IConfiguration` unless the file uses it. And keep the C#'s `IsNullOrWhiteSpace` vs `IsNullOrEmpty` distinction exactly — which of the two selects a fallback branch is behaviour. |
| `AddCustomStorageBasedLogConsistencyProvider("name")` | `builder.useMemoryJournaling()`. Thresh's journaling binder DETECTS a `CustomStorageInterface` host and installs the adaptor, so what Orleans named a log-consistency PROVIDER is the grain's own shape here; the journaling registration exists only to make the binder run, and the grain's durability still goes through its named grain-storage provider. |
| `UseInMemoryReminderService()` | `builder.useReminders()` — the no-argument form, whose default table is in-memory. |
| reading a registered storage provider back off the builder (to hand the SAME instance to a grain that takes storage by injection) | `builder.storageProvider(name)`, including the instance a convenience registration (`addPostgresStorage`, `addRedisStorage`) constructed for itself. Never register a second provider over the same database for the injected copy: two pools over one table is at best waste and at worst a split view. |
| `app.MapGrpcService<TService>()` | `server.addService(TServiceService, implementation)` on a `@grpc/grpc-js` `Server`, using the generated `*Service` descriptor. TWO differences bite. ASP.NET Core generates a base class per service, so a method the C# does NOT override still answers UNIMPLEMENTED; grpc-js keys handlers by name and a missing key becomes UNIMPLEMENTED only at call time, with no registration-time signal — so build a handler for EVERY method of the definition and synthesize the UNIMPLEMENTED answer for the ones the ported service leaves out. And ASP.NET constructs the gRPC service PER CALL, i.e. after the host has started, so a ported service that captures a grain factory in its constructor must be constructed after `SiloHost.start()`, not at wiring time. |
| `throw new RpcException(new Status(code, detail))` | throw an `Error` carrying grpc-js's `code`/`details`/`metadata` (a `ServiceError`), and convert it in the HOST wiring, not the service: a unary handler passes it to `callback(err)`, a server-streaming handler emits it (`call.emit("error", err)`), which grpc-js turns into a status and ends the stream. Anything that is not such an error becomes UNKNOWN, as an unhandled exception does in ASP.NET Core. |
| `app.MapGet("/", () => "...")` alongside gRPC | grpc-js owns its socket and cannot serve plain HTTP, where Kestrel multiplexes both over one port. Either drop the endpoint or run a tiny separate `node:http` listener on its own port — and keep the body text as a constant so it cannot drift. Never model it as a gRPC method. |
| `await foreach` over a **lazy** `IAsyncEnumerable` where the loop must classify what `MoveNextAsync` threw, or re-check cancellation between the move and the write | a MANUAL `[Symbol.asyncIterator]()` / `next()` / `finally { await it.return?.(); }` loop, not `for await`. A TypeScript async generator is lazy in exactly the way the C# iterator is — the body does not run until the first `next()` — so the throws the C# catches around `MoveNextAsync` still surface from inside the loop, and the try/catch must wrap the `next()` call rather than the loop set-up. `for await` can express neither the per-move catch nor a check between the move and the body. |

The DI row is where a mechanical port most often stalls. Orleans code leans on the container to
hide graph wiring; Thresh makes it explicit. Resolve this per package by writing one composition
function that builds the object graph, and have both the silo host and the tests call it.

## Testing

| Orleans | Thresh |
|---|---|
| `TestCluster` / `TestClusterBuilder` | `TestCluster` (`@thresh/testing/test-cluster`) |
| `TestCluster.Client` / `TestCluster.GrainFactory` | `await cluster.client` — a `ClientNode` outside every silo, gatewayed through the cluster's membership and registered with `TestClusterOptions.grains`. NOT the same as `cluster.getGrain`, which routes through the primary SILO: a call made there is issued by that silo and runs its outgoing call filters, which a client call does not. Port a `fixture.GrainFactory` / `Client` call site to `cluster.client` and a `grainFactory` resolved from a silo's own container to `cluster.getGrain`; collapsing the two merges cases the C# suite pins apart. The accessor is async (connecting is), it is created on FIRST access rather than at `start()` — a connected client registers in the client directory and opens a gateway connection, so an untouched one costs a message-counting test nothing — and `dispose()` closes it before stopping any silo, as Orleans' `StopAllSilosAsync` does. |
| a test silo configurator that deliberately never calls `UseInMemoryReminderService()`, to exercise the deployment shape with NO reminder service | `TestCluster.start({ reminders: false })`. `TestCluster` installs the cluster-wide `reminderTable` on every silo by default; `reminders: false` skips that so `runtime.registerReminder` fails with "reminders are not configured on this silo". Not the same as a reminder table whose writes reject — that tests a reminder service that FAILED. A single silo needing its own table can still call `builder.useReminders(table)` from `configureSilo`. |
| `[Fact]` / `[Theory]` + `[InlineData]` | `it(...)` / `it.each([...])` — and put a `%s` (or `%o`) placeholder in the title. xUnit derives a theory case's display name from its arguments automatically; vitest does not, so a title with no placeholder gives every row the SAME name and a failure no longer says which row failed. |
| `Assert.Equal(a, b)` | `expect(b).toEqual(a)` — note the argument order flips |
| `Assert.Same(a, b)` | `expect(b).toBe(a)` |
| `Assert.Throws<T>` / `ThrowsAsync<T>` | `expect(fn).toThrow(T)` / `await expect(p).rejects.toThrow(T)` |
| `Assert.True(condition)` / `Assert.False(condition)` | `expect(condition).toBe(true)` / `.toBe(false)` |
| `Assert.True(condition, message)` — the **message-carrying** overload | `expect(condition, message).toBe(true)`. Vitest takes the message as `expect`'s SECOND argument, not as a trailing argument to the matcher, and it is easy to drop on the way across. Do not drop it: in a property-style suite that asserts thousands of times inside nested loops (a reverse/forward cross-check, a generated-world sweep) the interpolated message naming the offending tuple is the ONLY thing that identifies which of the thousands failed — `expected false to be true` on its own is unactionable. Where the same message is built at many sites, wrap it once as a local `assertTrue(condition, message)` so the C#'s call shape ports line-for-line. |
| `IClassFixture<T>` | a `beforeAll`/`afterAll` pair, or a helper that returns the fixture |
| `ICollectionFixture<T>` + `[CollectionDefinition]` — one fixture instance shared by SEVERAL test CLASSES | a module exporting the fixture plus a helper that installs the `beforeAll`/`afterAll` pair on the importing file. Vitest isolates test FILES, so the shared-instance half does NOT port: each importing file evaluates its own copy of the module and gets its OWN fixture. Check whether the C# depended on the sharing (one seeded database read by every class) or only tolerated it (a container reused to save start-up time). Per-file instances are stronger isolation, never weaker, but they are also N containers — say which it is where the fixture is defined. |
| `[Collection("name")]` (non-parallel) | `describe.sequential(...)`, or a file-level sequential config |
| `Xunit.SkippableFact` / `Skip.If(condition)` | `it.skipIf(condition)` |
| `Skip.If(condition, reason)` — a skip whose **reason string is load-bearing** | `it(name, (ctx) => ctx.skip(condition, reason))`. `it.skipIf` takes no reason, so it silently drops the message; where the recorded reason is the point of the test (a quarantined fixture reporting why it is not run), the reason must survive into the runner output. |
| `[MemberData]` over a computed, possibly **empty** row set | `it.for(rows)(...)` — and keep the C#'s empty-set sentinel row. Zero rows is a failure in both runners (xUnit "No data found"; vitest "No test suite found in file"), so the sentinel is not a C# quirk to drop. `it.for` (not `it.each`) is the form that passes a `TestContext` to the case body. |
| Testcontainers | `testcontainers` (the Node port), same API shape. Two things do not carry across: probe for the runtime with a BOUNDED `Promise.race` around `getContainerRuntimeClient()` at module load (a suite that HANGS on a missing Docker endpoint is worse than one that fails, and such a suite usually lands in the default test project), and keep the C#'s NARROW catch — skip only on the no-endpoint signal (`Could not find a working container runtime strategy`), and let image-pull and container-start failures ESCAPE, so a Docker-enabled machine can never silently skip a durability gate. |
| Orleans `ServiceId` used to ISOLATE storage rows between tests | it isolates nothing. Thresh's storage providers key rows by `(grainId, stateName)` alone, with no service/cluster dimension, so `TestClusterOptions.serviceId` does not partition a shared database and two tests over the same singleton grain key COLLIDE. Give each test its own table (or its own database) instead, dropped in a `finally`. |

## Idioms that need judgement, not substitution

These are the places where a transliteration is wrong even when it compiles:

- **Structural equality.** C# records give value equality free. TypeScript objects do not.
  Every record used as a dictionary key, in a `HashSet`, or compared with `==` in the C# needs
  an explicit canonical-key function or a deep-equal, chosen once per type. A canonical key must
  be **unconditionally injective**, because the C# record equality it replaces is. Do not join on
  a separator "the grammar excludes" — length-prefix each field (`${part.length}:${part}`), or
  the key's correctness depends on a validator that usually lives in a different layer and often
  does not run. A separator that merely looks unusual is the same bug with a smaller blast radius.
- **C# record equality that is itself broken.** A record whose members include `byte[]`,
  `ImmutableList<T>` or `IReadOnlyDictionary<K,V>` compares those by reference, so its generated
  `==` is not value equality at all. Implementing real content equality in the port is a
  **sanctioned divergence** — it is the behaviour the C# meant — but say so at the site, because
  it is a deliberate difference from the source.
- **`switch` over a closed hierarchy.** C# pattern matching on a sealed record hierarchy becomes
  a discriminated union with a literal `kind` field and an exhaustiveness check in the default
  branch — write a local `assertNever(x: never): never`, since Thresh does not supply one — or
  the port loses the compiler's coverage guarantee. Condition the helper on **what the C#
  default arm actually did**, because the house rule is about compile-time coverage and never
  about changing runtime behaviour:
  - it threw a user-visible message — the helper takes `never` **and** throws that message; the
    two goals do not conflict.
  - it returned a **normal value** (`None`, an empty leaf, an empty set, `false`) — write a
    helper that takes `never` and *returns* that value:
    `function assertNeverEmpty(x: never): Result { return EMPTY; }`. Coverage is kept and the
    tolerant fallback survives. Reaching for the throwing `assertNever` here converts a
    deliberately tolerant default into a crash on exactly the inputs the C# was written to
    tolerate, and the compiler cannot warn you because the arm is unreachable in its view.
  - a union member is **live** and the C# deliberately falls through it (a `this`/`nil` child in
    a walk) — keep a plain do-nothing `default:`. It is not an exhaustiveness site at all, and
    an `assertNever` there throws on well-formed input.
- **Nullability.** C#'s `?` and TypeScript's `| undefined` mostly line up, but C# `null` and
  TypeScript `undefined` do not: pick `undefined` throughout and only use `null` where a wire
  format demands it.
- **Integer arithmetic.** C# `int` overflow wraps; JS `number` does not. Anything doing hash
  mixing, checksums, or bit manipulation must go through `Math.imul` / `| 0` / `>>> 0` or
  `BigInt`, and its tests must pin exact values.
- **String comparison and sorting.** `string.CompareOrdinal` is not `localeCompare`. Ordinal
  comparison is `a < b`; anything sorted for a wire-visible cursor must be ordinal. The
  comparator-free `Array.prototype.sort` is UTF-16 ordinal and so matches `StringComparer.Ordinal`
  exactly — that equivalence is what lets a ported `SortedSet` ordering stand unchanged, and it
  is worth stating at the site, because the next reader's instinct is to "fix" the bare sort by
  adding `localeCompare`, which is locale-dependent and reorders the cursor.
- **When a test contradicts the source, the source wins.** Under a green-tests gate the tempting
  move is to bend the port until the test passes. The C# is the authority: read it, and if the
  test is wrong, fix the test and say why at the site.
- **`struct` semantics.** A C# struct is copied on assignment. A TypeScript object is shared.
  A ported struct that the C# mutated after copying needs an explicit clone. The common shape is
  `list[i] = item with { X = y }` over a `readonly record struct`: the C# writes back a copy,
  while the naive TypeScript mutates an object other code still holds.
- **Absent versus empty.** C# `null` and an empty collection are distinct, and equality that
  normalises them together loses information. Keep `undefined` and `[]` distinct unless the C#
  itself conflated them. The shape this most often arrives in is a lazily allocated collection —
  `List<T>? xs = null;` with `xs ??= [];` at the first add — where the code later branches on
  `xs is null` (nothing was ever added) as a different state from `xs.Count == 0` (things were
  added and all removed). Port it as `T[] | undefined` initialised to `undefined`, allocating on
  first add exactly as the C# does. Initialising to `[]` for tidiness merges the two states, and
  the port then takes the "had some, all filtered out" branch on inputs that never had any.
