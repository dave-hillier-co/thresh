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
| `[AlwaysInterleave]` on a method | per-method reentrancy option in the grain's metadata |

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
  than a plain `Error`. Grep the C# for `[GenerateSerializer]` on anything deriving from
  `Exception` — each one is a registration in the port.
- **`byte[]` and text encoding.** `byte[]` becomes `Uint8Array`; `Encoding.UTF8.GetBytes` /
  `GetString` become `new TextEncoder().encode(...)` / `new TextDecoder().decode(...)`. Note that
  `Uint8Array` compares by reference, so any byte array inside a value type needs explicit
  content equality.
- **`decimal`, `long`/`ulong`, `DateTimeOffset`, `Guid`.** TypeScript `number` is a float64.
  `long`/`ulong` that can exceed 2^53 must become `bigint` or a string, and the choice has to be
  made once, at the value type, not per call site. `uint`/`ushort` fit in `number` exactly, so
  what is lost is the range invariant the C# type system enforced — restore it with a guard at
  the constructing factory. `DateTimeOffset` becomes an ISO string, epoch-millis `number`, or
  epoch-nanos `bigint`; pick one per field and write it down, and note that .NET's 100ns tick
  resolution (seven fractional digits) survives none of `Date`, epoch-millis, or `number`.
  `Guid` becomes `string`.

## Types and members

C# has several ways to attach a name to a value that TypeScript does not. Each has one right
answer, and using a different one per file is how a port becomes inconsistent.

| C# | TypeScript |
|---|---|
| `static class` used as a namespace | module-level exported functions, with the type name folded into the function name where it would otherwise be ambiguous (`AllowedRelationIdentity.Source` -> `allowedRelationSource`). There is no namespace object and no barrel. |
| a record instance property that is really a predicate | a free function (`Relation.IsPermission` -> `isPermission(relation)`) |
| a class whose only public member is one method, or a private constructor plus a static entry point | a module-private class and one exported free function (`Lexer.Tokenize` -> `tokenize`, `Parser.Parse` -> `parse`) |
| a **static singleton property** on an abstract record | a frozen module constant (`Object.freeze`), **never** a factory function — call sites compare it by reference, and a factory silently breaks every such match |
| a **default interface method** (an interface member with a body) | an exported free function named after the member (`defaultGreaterThan`), which implementations delegate to. Not a base class: the set of implementations is usually open across packages. |
| an **empty record** used as a proto placeholder | an interface with a phantom brand (`readonly __trait?: never`) plus a single frozen instance. A bare empty interface is structurally satisfied by every object, so the naive port compiles and is silently useless. |
| an enum with **explicit, proto-mirroring numeric values** | a string-literal union plus an explicit bidirectional wire map (`xToWire` / `xFromWire`, the latter returning `undefined` for unknown values). Never let the wire number ride on declaration order. |
| an enum that is not wire-visible | a string-literal union with no map |
| a **`[Flags]`** enum | stays **numeric** — the string-union row above would break every `(x & Flag) !== 0` call site. Transcribe a combined member (`All = A \| B`) literally rather than re-deriving it as "all the bits". |
| an instance **method** on a record | a free function, name-folded like the predicate row above (`Matches` -> `relationshipsFilterMatches`) |
| a static method **overload set** | two distinctly named functions, folding the distinguishing parameter type into each name |
| an instance property with a **computed body** | a **getter**, never a field snapshot. Porting it as a field is a silent behaviour change, and a TypeScript interface cannot require getter-ness — document and test-pin it. |
| a default **parameter** value | an absent optional member plus a named resolver using `??` (so an explicit `0`/`false` survives), not a default in the type |
| a value-tuple return `(ulong Count, bool Flag)` | a named `readonly interface` — a labelled TS tuple's labels are unchecked |

Two TypeScript settings change every signature mechanically. `exactOptionalPropertyTypes` means
a C# `?` member becomes `?: T | undefined`, not `?: T`. `noUncheckedIndexedAccess` means every
indexing expression already yields `T | undefined`, which is usually what the C# meant anyway.

## Exceptions

| C# | TypeScript |
|---|---|
| `ArgumentException` / `ArgumentNullException` | a project `InvalidArgumentError`. Keep the guard even where the parameter's TypeScript type is non-optional — the caller may be untyped. |
| `FormatException` | a project `FormatError` |
| `InvalidOperationException` | a project error class, named for the invariant it protects |
| `catch (Ex) when (cond)` | an exception filter. Read `cond` first: a constant-true filter is a plain `catch`, not a condition to port. |
| `new Ex(message, inner)` | `super(message, { cause: inner })` — the ES2022 option. Type `inner` as `unknown`: a caught binding need not be an `Error`. |

When registering an exception surrogate, encode **only** the carried data where the constructor
re-derives its message from it; encode the message only when it is the type's sole distinguishing
state.

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
| `long.Parse(s, InvariantCulture)` | throws on hex, exponent notation, empty input and out-of-range values; `BigInt(s)` accepts `0x2a` and is unbounded. Regex-validate the `NumberStyles.Integer` shape, then `BigInt`, then range-check against int64. |
| `DateTimeOffset` | 100ns tick resolution, seven fractional digits, and .NET Core rounds fractions **half away from zero** rather than truncating. `AddTicks` with negative instants needs an explicit floor adjustment, because C# `long` division and `BigInt` division both truncate toward zero. |
| `string.Trim()` / `char.IsWhiteSpace` | the .NET and JS whitespace sets genuinely differ (U+0085, U+FEFF). If the difference is wire-visible, hand-roll the .NET set. |
| `char.IsLetter` / `IsLetterOrDigit` / `IsDigit` | Unicode-aware; JS `\d` and `[a-z]` are not. Use `\p{L}` / `[\p{L}\p{Nd}]` / `\p{Nd}` with the `u` flag. A C# `char` is a UTF-16 code unit, so index by code unit, not code point. |
| a non-multiline `$` in a regex | .NET's `$` also matches immediately before a single trailing `\n`; JS's does not. Anchored expressions need `\n?$`. |
| `JsonSerializer.Serialize` | `JavaScriptEncoder.Default` escapes more than `JSON.stringify` does: `&`, `'`, `+`, `<`, `>`, backtick, **every** control character, DEL, and **every** non-ASCII UTF-16 code unit — as `\uXXXX` with **uppercase** hex, astral characters as two escaped surrogate halves. If the output is wire-visible, hand-roll it. |
| `JsonSerializer.Deserialize<T>` | returns `null` for the `null` literal, matches property names **case-sensitively** by default, skips unknown members silently, and **throws** on a type mismatch. `JSON.parse` shares none of that. |
| `SHA256.HashData` + `Convert.ToHexStringLower` | `createHash("sha256").update(b).digest("hex")` from `node:crypto` — WebCrypto's `subtle.digest` is async and unusable from a synchronous method |
| `Guid.NewGuid().ToString("n")` | `crypto.randomUUID().replace(/-/g, "")` — 32 lowercase hex, no dashes |

## Collections and LINQ

| C# | TypeScript |
|---|---|
| `IEnumerable<T>` | `Iterable<T>` or `readonly T[]` |
| `IAsyncEnumerable<T>` | `AsyncIterable<T>` — an `async function*` generator |
| `await foreach (var x in xs)` | `for await (const x of xs)` |
| `IReadOnlyList<T>` / `IReadOnlyDictionary<K,V>` | `readonly T[]` / `ReadonlyMap<K,V>` |
| `ImmutableArray<T>` / `ImmutableDictionary<K,V>` | `readonly T[]` / `ReadonlyMap<K,V>`, copied on write |
| `.Select` / `.Where` / `.Any` / `.All` | `.map` / `.filter` / `.some` / `.every` |
| `.FirstOrDefault()` | `arr[0] ?? undefined` — with `noUncheckedIndexedAccess`, indexing already yields `T \| undefined` |
| `.SingleOrDefault()` | explicit length check; there is no built-in |
| `.OrderBy(k)` | `[...xs].sort(byKey)` — `sort` mutates, so copy first. Note .NET `List<T>.Sort` is an **unstable** introsort while `Array.prototype.sort` is stable: whenever the comparator has ties, the two disagree. |
| `IEnumerable<T>` built with `yield return` | a returned **array**, not a `function*`. A C# `IEnumerable` is re-enumerable; a TS generator is single-pass and silently yields nothing the second time. Use `function*` only where the C# was itself single-pass. |
| `List<T>.GetRange(i, n)` | `xs.slice(i, i + n)` — but `GetRange` throws on a negative count while `slice` counts from the end. See the unsigned-range note under Serialization. |
| `.GroupBy(k)` | `Map.groupBy(xs, k)` |
| `.ToDictionary(k, v)` | `new Map(xs.map((x) => [k(x), v(x)]))` — but `ToDictionary`/`ToImmutableDictionary` **throw** on a duplicate key while `new Map(...)` and `map.set` silently overwrite. Where the C# relied on that throw, add an explicit check. |
| `Dictionary<K,V>` with a **struct/record key** | `Map` keys compare by reference — key by a canonical string, or the lookup silently misses. This is the single most common porting bug. |
| `Dictionary<K,V>` iteration order | .NET's is unspecified and disturbed by removals; a JS `Map` is insertion-ordered and stable. Usually benign — but it is often *why* the C# sorts defensively, so never "simplify away" such a sort. |
| `Dictionary<K, V?>` where null is meaningful | `Map<K, V | undefined>`: keep `has` and a defined `get` distinct, and never collapse the read with `??`. |
| `record with { ... }` | object spread `{ ...state, x }` — a fresh object, with untouched members still shared by reference and never mutated. |
| `HashSet<T>` of records | same problem: `Set<string>` over a canonical key |
| `SortedDictionary` / `SortedSet` | no equivalent; keep a sorted array + binary search, or sort on read |

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
| `Task.WhenAny(work, Task.Delay(Infinite, ct))` | `raceAbort(promise, signal)` (`@thresh/core/abort`), which resolves to the `ABORTED` sentinel instead of rejecting — **cancellation as a clean exit**. Porting this with `raceSignal` turns a `yield break` into a thrown `GrainCallAbortedError`. |
| `Task.FromResult(x)` | `Promise.resolve(x)` from a **non**-`async` method |
| `TaskCompletionSource<T>` | a `{ promise, resolve }` pair; `RunContinuationsAsynchronously` is a no-op in JS |
| `CancellationToken` on a **plain interface** (no grain, no ambient source) | a trailing `signal?: AbortSignal | undefined` parameter, kept in the C#'s positional slot |
| `IAsyncEnumerable<T>.WithCancellation(token)` | no equivalent — `AsyncIterable` has no signal channel. Check the signal in the loop body, and have the producer take it too. |
| `Task.WhenAll(xs)` | `Promise.all(xs)` |
| `Task.WhenAny(xs)` | `Promise.race(xs)` |
| `.ConfigureAwait(false)` | delete it |
| `SemaphoreSlim` | `AsyncSerialExecutor` (`@thresh/core/async-serial-executor`), but first check whether the grain's single-turn guarantee already makes it unnecessary |
| `lock (x)` | delete it — a grain activation is single-threaded per turn. Keep a lock **only** if the C# guarded state shared *between* grains, which is itself a smell the port should remove. |
| `RequestContext.Set/Get` | `ctx.runtime.setRequestContext(key, value)` / `getRequestContext(key)` — ambient across the call chain. **Values are `string` only**, where Orleans takes any serializable object: anything richer must be encoded at the set and decoded at the get. |
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

## Hosting and DI

| Orleans | Thresh |
|---|---|
| `Host.CreateApplicationBuilder().UseOrleans(b => ...)` | `createSilo({ ... })` |
| `builder.UseLocalhostClustering()` | static membership in the silo options |
| `builder.AddMemoryGrainStorage("name")` | `useMemoryStorage({ name })` |
| `services.AddSingleton<IFoo, Foo>()` | a module-level value, or a factory passed into `createSilo`. Thresh has no DI container; constructor injection becomes explicit wiring. |
| `[FromKeyedServices("name")] IGrainStorage` | resolve the named storage from the silo's storage registry and pass it in |
| `IOptions<TOptions>` | a plain options object with defaults applied at the call site |
| `ILogger<T>` | the logger from the grain context, or the module logger |

The DI row is where a mechanical port most often stalls. Orleans code leans on the container to
hide graph wiring; Thresh makes it explicit. Resolve this per package by writing one composition
function that builds the object graph, and have both the silo host and the tests call it.

## Testing

| Orleans | Thresh |
|---|---|
| `TestCluster` / `TestClusterBuilder` | `TestCluster` (`@thresh/testing/test-cluster`) |
| `[Fact]` / `[Theory]` + `[InlineData]` | `it(...)` / `it.each([...])` |
| `Assert.Equal(a, b)` | `expect(b).toEqual(a)` — note the argument order flips |
| `Assert.Same(a, b)` | `expect(b).toBe(a)` |
| `Assert.Throws<T>` / `ThrowsAsync<T>` | `expect(fn).toThrow(T)` / `await expect(p).rejects.toThrow(T)` |
| `IClassFixture<T>` | a `beforeAll`/`afterAll` pair, or a helper that returns the fixture |
| `[Collection("name")]` (non-parallel) | `describe.sequential(...)`, or a file-level sequential config |
| `Xunit.SkippableFact` | `it.skipIf(condition)` |
| Testcontainers | `testcontainers` (the Node port), same API shape |

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
  the port loses the compiler's coverage guarantee. Where the C# default branch already threw a
  user-visible message, the helper takes `never` **and** throws that message; the two goals do
  not conflict.
- **Nullability.** C#'s `?` and TypeScript's `| undefined` mostly line up, but C# `null` and
  TypeScript `undefined` do not: pick `undefined` throughout and only use `null` where a wire
  format demands it.
- **Integer arithmetic.** C# `int` overflow wraps; JS `number` does not. Anything doing hash
  mixing, checksums, or bit manipulation must go through `Math.imul` / `| 0` / `>>> 0` or
  `BigInt`, and its tests must pin exact values.
- **String comparison and sorting.** `string.CompareOrdinal` is not `localeCompare`. Ordinal
  comparison is `a < b`; anything sorted for a wire-visible cursor must be ordinal.
- **When a test contradicts the source, the source wins.** Under a green-tests gate the tempting
  move is to bend the port until the test passes. The C# is the authority: read it, and if the
  test is wrong, fix the test and say why at the site.
- **`struct` semantics.** A C# struct is copied on assignment. A TypeScript object is shared.
  A ported struct that the C# mutated after copying needs an explicit clone. The common shape is
  `list[i] = item with { X = y }` over a `readonly record struct`: the C# writes back a copy,
  while the naive TypeScript mutates an object other code still holds.
- **Absent versus empty.** C# `null` and an empty collection are distinct, and equality that
  normalises them together loses information. Keep `undefined` and `[]` distinct unless the C#
  itself conflated them.
