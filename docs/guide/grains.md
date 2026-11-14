# Grains and calls

## Identity and keys

Declare the key kind on the definition — `{ key: "integer" }` on `defineGrain` or
`defineGrainInterface`, one of `"string"` (the default), `"integer"`, `"guid"`, `"guid-compound"`,
or `"integer-compound"`. A separately declared contract can instead intersect `GrainKey<string>`,
`GrainKey<bigint>`, `GrainKey<Guid>`, or a compound marker; both paths resolve to the same key type,
so existing marked interfaces keep working unchanged. The definition is passed to `getGrain`; the
returned proxy implements the contract.

## Functional lifecycle

`defineGrain(name, setup)` runs `setup` once per activation. What `setup` returns is the grain's
message surface and nothing else — the interface methods, plus an optional self incoming-call filter
under `INCOMING_CALL_FILTER`. Lifecycle is registered with hooks, like the state facets:
`useOnActivate(handler)` and `useOnDeactivate(handler)`. Both compose — call them as often as you
like, from `setup` or from helpers it calls. Activate hooks run in registration order; deactivate
hooks unwind LIFO, so a hook always tears down before whatever it was set up on top of. Do not leak
closure state outside the grain.

The definition it returns **is** the grain's interface: the message surface is inferred from what the
factory returns, so `registerGrain(definition)` and `getGrain(definition, key)` both take it and
there is no separate interface to declare. This couples callers to the implementation module, which
is the right trade-off in-process and the wrong one across a process boundary — declare those
contracts separately with `defineGrainInterface` and register with
`registerGrain(ctor, { interfaces })`, which also stays the form for one grain under several
interfaces or per-silo interface versions.

Interface ids are derived from the interface *name*, so renaming one is a wire break. Any grain that
has ever been deployed must pin `{ interfaceName: "..." }`.

Each normal method call is one serialized turn. `readOnly` interface options declare calls that do
not mutate state and enable runtime optimizations; violating read-only state guards throws. Other
method options include response timeout, transaction behavior, interleaving, and one-way delivery.

## Class form and functional form

A grain is written either as a class (decorators) or as a `defineGrain` factory (options and
hooks); both register through the same catalog, scheduler and facet binding, and the functional form
is the recommended one for new code. The two are kept in lockstep: every authoring feature is in the
matrix in `packages/core/src/authoring-matrix.test.ts`, which is checked against what
`@thresh/core/decorators` and `@thresh/core/define-grain` export, and fails when a decorator or hook
is added to one without an answer for the other.

| Feature | Class form | Functional form |
|---|---|---|
| Registration | `@grain()` | `defineGrain(name, factory, options)` |
| Full reentrancy | `@reentrant()` | `reentrant: true` |
| May-interleave predicate | `@mayInterleave(predicate)` | `useMayInterleave(predicate)` |
| Implicit stream subscription | `@implicitStreamSubscription(ns)` | `implicitSubscriptions: [ns]` |
| Implicit channel subscription | `@implicitChannelSubscription(ns)` | `implicitChannelSubscriptions: [ns]` |
| Regex channel subscription | `@regexImplicitChannelSubscription(re)` | `regexImplicitChannelSubscriptions: [re]` |
| Persistent, reducer, transactional, durable state | `@persistentState` and the other field decorators | `usePersistentState` and the other `use*` hooks |
| Third-party facet | `@grainFacet(kind, name, config)` | `useGrainFacet(kind, { name, config })` |
| Activation and deactivation | override `onActivate` / `onDeactivate` | `useOnActivate` / `useOnDeactivate` |
| Migration state | implement `onDehydrate` / `onRehydrate` | `useOnDehydrate` / `useOnRehydrate` |
| Per-silo dependencies | a `GrainActivator` passes constructor arguments | `useDependency(token)` |
| Durable job handler | `DURABLE_JOB_HANDLER` member | `useDurableJobHandler` |

A few notes on the functional column:

- `useMayInterleave` is per activation, so the predicate may close over the factory's state.
- `useGrainFacet` returns a handle; `handle.get()` is the facet the factory registered with
  `addGrainFacetFactory(kind, name, factory)`, bound before `onActivate`.
- `useOnDehydrate` and `useOnRehydrate` make the activation a migration participant without
  putting `onDehydrate` / `onRehydrate` on the grain's call surface: a remote call to either is
  refused like any runtime hook, and a factory that returns one of them is rejected at activation.
  A hook runs synchronously, as the class methods do.
- `useDependency` reads a value the silo's builder registered with
  `builder.addGrainDependency(token, value)`, where `token` comes from `createDependencyToken<T>(name)`
  in `@thresh/core/grain-dependency`. It is per silo: two silos in one process can supply different
  values. It throws, naming the token, if the silo registered none.
- All of them follow the rules of hooks: call them at the top of the factory, synchronously.

Three features are deliberately single-path:

- **`JournaledGrain`** is a base class whose transition and log-view hooks the application
  overrides; a factory has no base class. A functional grain journals with `useDurable*` or folds
  events with `useReducerState`.
- **Custom log-view storage** (`readStateFromStorage`, `applyUpdatesToStorage`, `clearStoredState`)
  is an extension point of `JournaledGrain` and follows it.
- **`defineReducerGrain`** is a whole-grain shortcut over `useReducerState`; a class grain gets the
  same behaviour from a `@reducerState` field.

## Interleaving

A turn is the whole `async` method, including every continuation after an `await`. By default a
grain runs one turn at a time: another call waits until the running turn's promise settles. These
let a call run while another turn is suspended at an `await`:

| Mechanism | Admits an incoming call while a turn is running when |
|---|---|
| `reentrant: true` on the grain (`@reentrant()`) | always |
| `alwaysInterleave` on the incoming method | always, whatever is running |
| `readOnly` on the incoming method | the running blocking turn is also `readOnly` — a `readOnly` call still waits behind a write |
| `mayInterleave` predicate on the grain | the predicate accepts the incoming call or the running one |
| `RequestContext.allowCallChainReentrancy(fn)` in a running turn | the incoming call is on the call chain that turn opened inside `fn` |

What an interleaved call may observe is a contract, pinned by
`packages/runtime/src/interleaving.test.ts`:

- It runs only while the other turn is suspended at an `await`, never concurrently with it. The code
  between two `await`s of a turn runs to completion before any other turn of the activation starts
  or resumes, so everything the suspended turn wrote before its `await` happens-before the
  interleaved call's first read.
- It sees exactly that: the suspended turn's state as of its last `await`, including writes the turn
  has not finished. The runtime does not hide a write's intermediate state. A grain whose
  interleaving reads must not see one keeps the in-progress write out of the fields those reads use
  and publishes it in one synchronous step after its last `await`. `JournaledGrain`'s `state` is
  such a confirmed view: it moves in one step once an append is durable, never through part of a
  batch.
- Methods without an interleave option never interleave each other, so a sequence of them stays
  atomic across its `await`s.

### Call-chain reentrancy

Call-chain reentrancy is off by default, as in Orleans. A non-reentrant grain that calls back into
itself — directly, or through other grains (`A -> B -> A`) — waits on its own running turn and fails
with `GrainCallTimeoutError` when the call's response timeout elapses. To allow it, wrap the calls in
`RequestContext.allowCallChainReentrancy` (Orleans' `AllowCallChainReentrancy`):

```ts
import { RequestContext } from "@thresh/core/request-context";

const total = await RequestContext.allowCallChainReentrancy(() => b.sumVia(selfKey));
```

While the callback's promise is pending, this activation admits calls on that chain, so `B`'s call
back into `A` runs during `A`'s `await`. Only the grain that opened the section is re-enterable:
`A -> B -> A -> B` still waits on `B` unless `B` opened its own section. The chain's id rides every
call made inside the callback and onward from there.
`RequestContext.suppressCallChainReentrancy(fn)` runs `fn` with no chain id, so its calls start
afresh.

## What a caller can invoke

A call names a method, so a grain type has a fixed set of remotely callable methods and only those
names are dispatched. Anything else is rejected with a "has no method" `GrainCallError` before the
grain instance is touched, whether the caller is another grain, a silo or an external gateway client.

- Functional grains: the members of the object the factory returns.
- Class grains: methods on the class and on any base class you wrote, plus methods declared as
  class-field arrow functions. Members of the framework base classes (`Grain`, `JournaledGrain`)
  are never callable, so `raiseEvent`, `clearLog`, `confirmEvents` and `installLogViewAdaptor`
  stay inside the grain.
- Never callable, whatever you name them: `constructor`, the runtime's own hooks (`onActivate`,
  `onDeactivate`, `onDehydrate`, `onRehydrate`, `setContext`), the custom-storage members
  (`readStateFromStorage`, `applyUpdatesToStorage`, `clearStoredState`) and anything inherited
  from `Object.prototype`.

TypeScript `private` and `protected` are erased at runtime, so they do not hide a method: a
`private` method on the class is still in the callable set. What hides a helper is keeping it out
of the set: a module-level function, a closure variable in a functional grain (not returned from
the factory), or a `#private` method. Client-hosted observers follow the same rule: only the
object's own and user-prototype methods.

A reference never turns a JavaScript protocol name into a call: `then`, `toJSON`, `valueOf`,
`toLocaleString`, `constructor`, `asymmetricMatch`, `nodeType` and `$$typeof` read as `undefined`,
so `JSON.stringify`, `await` and test matchers do not reach the grain. `String(ref)` answers
locally with the grain type and key. Do not name a grain method after one of these.

## Calls and failure

Grain references are location transparent and serializable. Calls may fail because application code
throws, a deadline expires, cancellation propagates, load is shed, or a node disappears. Design
commands to be idempotent where retry is possible. Never rely on a specific activation or silo.

Use `RequestContext` for small cross-cutting string headers, incoming/outgoing call filters for
logging or policy, and `GrainCancellationToken` for cancellation across a grain boundary. Class
grains and decorators remain supported for Orleans ports; prefer functional grains for new code.
