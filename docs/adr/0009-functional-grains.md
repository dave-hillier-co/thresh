# ADR 0009 — Functional grains (factory closures instead of classes)

- Status: Accepted — `defineGrain` + `useReducerState` / `usePersistentState` shipped, with a
  functional `examples/bank` account grain and an end-to-end test. **Functional authoring is now the
  documented default**; the class + decorator style is retained as the runtime substrate and an
  interop surface (see [02](../02-actor-model.md), [11](../11-public-api-and-examples.md)).
- Context docs: [02 — The actor model](../02-actor-model.md),
  [07 — Persistence](../07-persistence.md), [ADR 0001](0001-runtime-proxy-grain-references.md),
  [ADR 0006](0006-reducer-grains.md)

## Context

The shipped authoring model is a **class with decorators**: a grain `extends Grain`, is registered
with `@grain()`, reaches its identity and services through `this` (`this.id`, `this.runtime`,
`this.getGrain`), declares facets with field decorators (`@persistentState`, `@reducerState`), keeps
per-activation state in fields, and overrides `onActivate` / `onDeactivate`. This is a deliberate
1:1 mapping onto Orleans (every doc cites the corresponding Orleans source), and that mapping is a
documented design value.

React is direct prior art for moving the *same* programming model off classes: it replaced lifecycle
class components with **function components + hooks** (`useState`, `useReducer`, `useEffect`) without
changing what a component *is*. The forces that drove that move apply to grain authoring in TS/JS:

- **Closures encapsulate per-activation state more naturally than `this`.** A field needs
  `private state!: PersistentState<T>` — a definite-assignment `!` asserting the runtime binds it
  later. A closure variable is just a `const`, captured by the methods that use it. The single-turn
  guarantee ([02](../02-actor-model.md)) makes closure variables exactly as lock-free as fields.
- **The field-decorator facet injection is the fiddly part.** `@persistentState` / `@reducerState`
  register per-instance metadata through a `ClassFieldDecoratorContext.addInitializer` dance, and
  TC39 decorators are still settling (the repo pins SWC's `2022-03` proposal to compile them at all —
  see [12](../12-project-structure-and-tooling.md)). A plain function call avoids that surface.
- **The reducer grain is already half-functional.** Its pure `initial` / `reduce` are passed as
  options ([ADR 0006](0006-reducer-grains.md)); only the command-handler *shell* is a class. React's
  reducer hook is explicitly the analogy that ADR cites.

The question this ADR settles: should the runtime offer a functional authoring API, and is it
compatible with the existing machinery? It is — the shell is the only thing that changes.

## Decision

Offer a **functional authoring API** layered above the existing runtime exactly as reducer grains
were layered above persistence, and make it **the default authoring style**. The class + decorator
grain remains valid — it is what `defineGrain` is built on — but it is documented as the substrate /
interop surface rather than the shape a developer reaches for first.

**The model:**

- `defineGrain(name, factory, options?)` replaces `@grain()`. The `factory` receives an explicit
  `ctx: GrainSetup` — the same surface the `Grain` base exposes through `this` (`id`, `runtime`,
  `getGrain`) — and returns the interface methods plus optional `onActivate` / `onDeactivate` hooks.
  The factory runs **once per activation**, after the context is bound and before any facet read.
- Per-activation state lives in **closure variables**, not fields. Sibling methods call each other
  directly through the closure, not through `this`.
- **Facet hooks** replace the field decorators: `useReducerState(ctx, name, { initial, reduce })`
  and `usePersistentState(ctx, name, opts)`. Each registers the *same per-instance field metadata*
  the decorator registers and returns a lazy handle; the runtime's existing binder fills the facet
  before `onActivate`, so the handle is live by the time any method runs (it throws if read earlier —
  the same effective contract as a class field being unbound before activation).

**Explicit `ctx` threading over an ambient hook-dispatcher.** React hides the "current fiber" because
it re-runs components on every render and needs stable hook ordering; a grain factory runs *once*, so
there is no rules-of-hooks ordering hazard and nothing to hide. Threading `ctx` is obvious and
unit-testable, and the codebase already reserves its `AsyncLocalStorage` for the invocation context.

**No runtime change.** `defineGrain` produces a `Grain` subclass that registers and activates through
the same catalog, turn scheduler, `Proxy` references ([ADR 0001](0001-runtime-proxy-grain-references.md))
and facet-binding machinery as a class grain. The catalog, persistence and hosting layers are
untouched; the two styles coexist and a grain opts in by how it is written. Pass either to
`registerGrain`.

## Rationale

- **Closures fit JS better than `this` for activation state.** No definite-assignment asserts, no
  `this`-binding hazards, and the same lock-free safety the turn model already provides.
- **It drops the decorator facet machinery** (`addInitializer`, the stage-3 decorator dependency) for
  a plain function call, while reusing the binder behind it unchanged.
- **It is additive and reuses everything.** The functional `examples/bank` account shares
  `initialAccount` / `reduceAccount` *verbatim* with the class version and produces identical results
  through the same runtime — concrete evidence that the reducer model is orthogonal to the shell, as
  [ADR 0006](0006-reducer-grains.md) argued.
- **It mirrors a migration the ecosystem has already validated** (React class → hooks), so the shape
  is familiar to the TS/JS developers this project targets.

## Consequences

- **It is an authoring shape, not a model change — Orleans parity is untouched.** Orleans has no
  functional/hooks authoring, so this differs from Orleans' *class + attributes* surface; but the
  grain it produces is the same virtual actor, with identical activation, single-turn, reentrancy and
  lifecycle semantics. This is exactly the project's stated split — a *faithful programming model*
  expressed *idiomatically in TypeScript* ([01 — goals](../01-overview-and-goals.md)) — the same kind
  of move as runtime `Proxy` references or the Kubernetes membership swap: the guarantee is identical,
  the way you express it differs. [02](../02-actor-model.md), [07](../07-persistence.md) and
  [11](../11-public-api-and-examples.md) now lead with the functional style and keep the class form as
  a short interop note; the Orleans source citations remain, since the runtime they map onto is
  unchanged.
- **The factory runs once per activation, not per render.** Hooks must be called in the factory body
  (so facet metadata is registered before the pre-activation read), but there is no cross-render
  ordering constraint — the rules-of-hooks tension that React carries does not arise here.
- **Reminders and streams need no new hooks today** — `ctx.runtime` exposes `registerReminder` /
  `getStreamProvider`, and `receiveReminder` is just another returned method. `useReminder` /
  `useTimer` sugar is optional future work, as is a `useActivate` effect form of the lifecycle hooks.
- **Two authoring styles to maintain**, but only one to teach: the docs lead functional and treat the
  class as interop, so the new surface stays the small one a reader learns (`defineGrain` + the facet
  hooks) while the class substrate is documented where it is actually needed.

## Alternatives considered

1. **Ambient hook dispatcher (React-style).** A module-level "current activation" that free-function
   hooks read, so the factory needs no `ctx` parameter. Matches React ergonomics and the repo already
   uses `AsyncLocalStorage` for the invocation context — but with no re-render there is nothing the
   ordering discipline buys, and hiding `ctx` trades testability for magic. Explicit threading wins.
2. **Full rewrite to functional (drop classes).** Maximally consistent, but discards the Orleans
   mapping, churns every doc/example/ADR, and commits to an API before its ergonomics are proven on
   real grains. Additive coexistence de-risks the bet.
3. **A declarative / generated grain layer above the class (the codegen path hinted in
   [ADR 0006](0006-reducer-grains.md)).** Orthogonal, not mutually exclusive: such a layer could sit
   above either shell. Not a substitute for choosing the hand-written authoring shape.

## Follow-on

[ADR 0010](0010-message-dispatch-reducer-grains.md) builds directly on this: a `defineReducerGrain`
specialization whose only surface is `dispatch(action)` + `query()`, which removes the *per-grain
method table* (`defineGrainInterface`) entirely — the "skip generating code" end state — by treating
the grain as a `useReducer`-style message-dispatch loop.
