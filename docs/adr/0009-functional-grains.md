# ADR 0009 — Functional grains (factory closures instead of classes)

- Status: Accepted — `defineGrain` + `useReducerState` / `usePersistentState` shipped; **functional
  authoring is the documented default**, with the class + decorator style retained as the runtime
  substrate and interop surface ([02](../02-actor-model.md), [11](../11-public-api-and-examples.md)).
- Context docs: [02](../02-actor-model.md), [07](../07-persistence.md),
  [ADR 0001](0001-runtime-proxy-grain-references.md), [ADR 0006](0006-reducer-grains.md)

## Context

The original authoring model is a **class with decorators**: `extends Grain`, `@grain()`, identity and
services via `this`, facets via field decorators (`@persistentState`, `@reducerState`), state in
fields, overridden `onActivate`/`onDeactivate` — a deliberate 1:1 mapping onto Orleans. React is direct
prior art for moving the *same* model off classes (class components → function components + hooks
without changing what a component *is*), and the same forces apply: closures encapsulate per-activation
state more naturally than `this` (a `const` capture vs. a definite-assignment `private state!: …`, and
the single-turn guarantee makes closures exactly as lock-free as fields); the field-decorator facet
injection (`addInitializer`, the still-settling TC39 decorators the repo pins SWC to compile) is the
fiddly part a plain function call avoids; and the reducer grain is already half-functional (its pure
`initial`/`reduce` are options; only the shell is a class).

## Decision

Offer a **functional authoring API** layered above the existing runtime (as reducer grains were layered
above persistence) and make it the **default**; the class form remains valid as the substrate
`defineGrain` is built on.

- `defineGrain(name, factory, options?)` replaces `@grain()`. The `factory` receives an explicit
  `ctx: GrainSetup` (the surface the `Grain` base exposes through `this`: `id`, `runtime`, `getGrain`)
  and returns the interface methods plus optional lifecycle hooks. It runs **once per activation**,
  after the context is bound and before any facet read.
- Per-activation state lives in **closure variables**; siblings call each other through the closure.
- **Facet hooks** (`useReducerState`, `usePersistentState`) replace field decorators, registering the
  *same per-instance metadata* and returning a lazy handle the existing binder fills before
  `onActivate` (it throws if read earlier — the same contract as an unbound class field).
- **Explicit `ctx` threading**, not an ambient hook-dispatcher: a factory runs *once*, so there is no
  rules-of-hooks ordering hazard to hide, and threading `ctx` is obvious and unit-testable.
- **No runtime change** — `defineGrain` produces a `Grain` subclass that registers and activates
  through the same catalog, scheduler, `Proxy` references and facet binder; the two styles coexist and
  a grain opts in by how it is written.

## Consequences

- **An authoring shape, not a model change — Orleans parity is untouched.** Orleans has no functional
  authoring, so this differs from its class+attributes surface, but the grain produced is the same
  virtual actor with identical activation/turn/reentrancy/lifecycle semantics — the project's stated
  "faithful model, idiomatic TypeScript" split ([01](../01-overview-and-goals.md)), like `Proxy`
  references or the Kubernetes swap. The docs lead functional and keep the class as a short interop
  note; Orleans citations remain since the runtime is unchanged.
- The functional and class `examples/bank` accounts share `initial`/`reduce` verbatim and produce
  identical results — evidence the reducer model is orthogonal to the shell.
- Reminders/streams need no new hooks (`ctx.runtime` exposes them; `receiveReminder` is just a returned
  method); `useReminder`/`useTimer` sugar is optional future work.
- Two authoring styles to maintain, but only one to teach.

## Alternatives considered

- **Ambient hook dispatcher (React-style)** — matches React ergonomics, but with no re-render the
  ordering discipline buys nothing and hiding `ctx` trades testability for magic. Rejected.
- **Full rewrite to functional** — discards the Orleans mapping and commits to an API before it is
  proven; additive coexistence de-risks it. Rejected.
- **A generated grain layer** — orthogonal (could sit above either shell), not a substitute for
  choosing the hand-written shape.

## Follow-on

[ADR 0010](0010-message-dispatch-reducer-grains.md) builds on this: a `defineReducerGrain` whose only
surface is `dispatch(action)` + `query()`, removing the per-grain method table entirely.
