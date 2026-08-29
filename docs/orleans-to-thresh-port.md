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
| `IGrainObserver` | **no equivalent.** Thresh has `ObserverManager` — the snapshot/TTL fan-out COLLECTION ported from Orleans — but no observer REFERENCE type, so there is nothing for a grain method to receive and later call back on. A port that needs the push side must stop here and fix Thresh; the shape of the observer interface and its `[OneWay]` options do port, so the declaration can land ahead of the runtime. |

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
| a default **parameter** value | an absent optional member plus a named resolver using `??` (so an explicit `0`/`false` survives), not a default in the type |
| `bool TryX(string? input, out T result)` | a function returning `T \| undefined`. Read what the C# does on each path FIRST: the common shape returns `false` for absent/empty input and **throws** for malformed input, and those two are not the same answer. Collapsing the throw into `undefined` turns a corrupted wire token into "start from the beginning", which silently restarts a paged read instead of rejecting it. |
| a value-tuple return `(ulong Count, bool Flag)` | a named `readonly interface` — a labelled TS tuple's labels are unchecked |
| a **mutable public auto-property** on an otherwise-immutable object (`IDispatcher Dispatcher { get; set; }` defaulting to `this`) | a **class** with a public mutable field. This is the case where the prefer-`interface`-and-`readonly` rule points the wrong way: the property is a seam that tests and composition roots reassign **on a live object**, so a frozen literal, a `readonly` member or a constructor-only option makes the seam unusable and forces the port to invent a different wiring than the source's. |
| a nested sealed record hierarchy whose nested types **re-declare** the base's positional members | one discriminated union whose members each carry those members in full — the re-declaration is not extra state, so do not model it as a base object plus a nested one. An abstract-typed `node with { Caveat = c }`, which C# dispatches to the concrete clone, is then just `{ ...node, caveat: c }`; that is only correct while the discriminant is a plain data field, so never compute `kind` in a getter or method. A spread copies own enumerable properties only, so a `kind` living on a getter or a prototype is silently dropped by `{ ...node }` and the clone stops matching any arm of the switch it was cloned to feed — the declared type is still the union, so nothing warns and the loss surfaces at runtime as an unexplained default-arm hit. |
| a private field `_x` | `#x`, and `private readonly x` **only** where a constructor parameter property is the natural spelling. Pick one per class rather than mixing: `#x` is genuinely inaccessible at runtime while `private` is erased, so a half-and-half class leaves it unclear which members a test is allowed to reach and which the port is free to rename. |

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
| a filter listing **BCL exception types** (`when (ex is CelException or InvalidOperationException or ArgumentException)`) | the port has usually collapsed those into fewer classes, so match the **mapped project errors plus a plain `Error`** — the ported layer beneath often throws a plain `Error` where .NET threw `InvalidOperationException` — and **rethrow** `TypeError`, `RangeError`, `EvalError` and any non-`Error` throw. Catching everything converts a programming fault into the source's user-facing error and hides the bug; catching only the mapped classes misses the path the port itself now throws on, so the C#'s tolerant branch never runs. |
| `new Ex(message, inner)` | `super(message, { cause: inner })` — the ES2022 option. Type `inner` as `unknown`: a caught binding need not be an `Error`. |
| the Orleans transport family — `SiloUnavailableException`, `OrleansMessageRejectionException`, `TimeoutException`, and `OrleansException` as the catch-all base | no one-to-one names. `RejectionError` (its `kind` names the refusal, so both silo-unavailable and message-rejected land here), `GrainCallTimeoutError`, and `GrainCallError` as the general dispatch/execution failure standing in for the `OrleansException` base. **Thresh has no common base class beneath the three**, so a C# `OrleansException` catch-all becomes an explicit list, and a new Thresh error class will not be picked up by it automatically. Never widen the arm to a bare `Error`: `TypeError`/`RangeError` must stay programming faults, not retriable transport failures. |
| a filter around a **ported parse** — `when (ex is FormatException or OverflowException or ArgumentException)` | match what the PORTED parser actually throws, which is often a JS built-in: a hand-rolled `NumberStyles` check typically throws `SyntaxError` for the shape and `RangeError` for the out-of-range value. This is the one place the "rethrow `TypeError`, `RangeError`" rule above is **wrong** — there `RangeError` is a programming fault, here it is the source's `OverflowException` and must be caught. Decide it from the throw sites of the specific parse being called, and say so at the catch. |
| `OperationCanceledException` (and `TaskCanceledException`, which derives from it) | TypeScript has no such hierarchy, so one C# `is OperationCanceledException` becomes a predicate over the **abort family**: `GrainCallAbortedError`, `GrainTaskCanceledError`, and a DOM `DOMException` whose `name` is `"AbortError"`. |

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
| `IEnumerable<T>` built with `yield return` | a returned **array**, not a `function*`. A C# `IEnumerable` is re-enumerable; a TS generator is single-pass and silently yields nothing the second time. Use `function*` only where the C# was itself single-pass. |
| `List<T>.GetRange(i, n)` | `xs.slice(i, i + n)` — but `GetRange` throws on a negative count while `slice` counts from the end. See the unsigned-range note under Serialization. |
| `.GroupBy(k)` | `Map.groupBy(xs, k)` |
| `.ToDictionary(k, v)` | `new Map(xs.map((x) => [k(x), v(x)]))` — but `ToDictionary`/`ToImmutableDictionary` **throw** on a duplicate key while `new Map(...)` and `map.set` silently overwrite. Where the C# relied on that throw, add an explicit check. |
| `Dictionary<K,V>` with a **struct/record key** | `Map` keys compare by reference — key by a canonical string, or the lookup silently misses. This is the single most common porting bug. |
| `Dictionary<K,V>` iteration order | .NET's is unspecified and disturbed by removals; a JS `Map` is insertion-ordered and stable. Usually benign — but it is often *why* the C# sorts defensively, so never "simplify away" such a sort. Ask per sort whether its order is **observable** — does it feed a cursor, a first-match short-circuit, a truncated page, a returned sequence? Where it is, keep the sort verbatim even though the `Map` is already ordered, because insertion order and the C#'s sort key are not the same order and the difference is a wire-visible change. Where nothing downstream can see it the sort is merely defensive, and it still costs nothing to keep. |
| `Dictionary<K, V?>` where null is meaningful | `Map<K, V | undefined>`: keep `has` and a defined `get` distinct, and never collapse the read with `??`. |
| `record with { ... }` | object spread `{ ...state, x }` — a fresh object, with untouched members still shared by reference and never mutated. A `static readonly` struct instance that call sites copy with `with` becomes a frozen module constant (see the static-singleton row) **plus a spread at every site**: the naive port assigns into the shared constant instead, and without `Object.freeze` that write succeeds and corrupts every later reader, far from the line that made it. |
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
| `[Fact]` / `[Theory]` + `[InlineData]` | `it(...)` / `it.each([...])` — and put a `%s` (or `%o`) placeholder in the title. xUnit derives a theory case's display name from its arguments automatically; vitest does not, so a title with no placeholder gives every row the SAME name and a failure no longer says which row failed. |
| `Assert.Equal(a, b)` | `expect(b).toEqual(a)` — note the argument order flips |
| `Assert.Same(a, b)` | `expect(b).toBe(a)` |
| `Assert.Throws<T>` / `ThrowsAsync<T>` | `expect(fn).toThrow(T)` / `await expect(p).rejects.toThrow(T)` |
| `Assert.True(condition)` / `Assert.False(condition)` | `expect(condition).toBe(true)` / `.toBe(false)` |
| `Assert.True(condition, message)` — the **message-carrying** overload | `expect(condition, message).toBe(true)`. Vitest takes the message as `expect`'s SECOND argument, not as a trailing argument to the matcher, and it is easy to drop on the way across. Do not drop it: in a property-style suite that asserts thousands of times inside nested loops (a reverse/forward cross-check, a generated-world sweep) the interpolated message naming the offending tuple is the ONLY thing that identifies which of the thousands failed — `expected false to be true` on its own is unactionable. Where the same message is built at many sites, wrap it once as a local `assertTrue(condition, message)` so the C#'s call shape ports line-for-line. |
| `IClassFixture<T>` | a `beforeAll`/`afterAll` pair, or a helper that returns the fixture |
| `[Collection("name")]` (non-parallel) | `describe.sequential(...)`, or a file-level sequential config |
| `Xunit.SkippableFact` / `Skip.If(condition)` | `it.skipIf(condition)` |
| `Skip.If(condition, reason)` — a skip whose **reason string is load-bearing** | `it(name, (ctx) => ctx.skip(condition, reason))`. `it.skipIf` takes no reason, so it silently drops the message; where the recorded reason is the point of the test (a quarantined fixture reporting why it is not run), the reason must survive into the runner output. |
| `[MemberData]` over a computed, possibly **empty** row set | `it.for(rows)(...)` — and keep the C#'s empty-set sentinel row. Zero rows is a failure in both runners (xUnit "No data found"; vitest "No test suite found in file"), so the sentinel is not a C# quirk to drop. `it.for` (not `it.each`) is the form that passes a `TestContext` to the case body. |
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
