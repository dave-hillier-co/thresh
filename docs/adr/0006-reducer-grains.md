# ADR 0006 — Reducer grains (event-routed, immutable state)

- Status: Accepted — implemented (`@reducerState`)
- Context docs: [02 — The actor model](../02-actor-model.md),
  [07 — Persistence](../07-persistence.md), [09 — Event streams](../09-event-streams.md)

## Context

The shipped persistence model is a **mutable state facet**: a grain reads `@persistentState`, mutates
`state.value` in place, and calls `state.write()` with etag optimistic concurrency
([07](../07-persistence.md)). It is simple and is the Orleans default, but it mixes *what happened*
with *the current value*, and state transitions are scattered across imperative mutations.

The **reducer** shape — which the authors prefer — separates the two: a command produces past-tense
**events**, and an immutable state value is folded from them by a **pure reducer**
`reduce(state, event) => state`. It is worth being precise that this is a *state-management* pattern
in its own right, **not** event sourcing. React/Redux uses exactly this shape with `useReducer`:
actions are routed through a pure reducer to produce new immutable state, and the actions are
**transient** — they are not stored; what you keep is a snapshot of the reduced state.

Orleans is direct prior art for the model: `JournaledGrain<TState, TEvent>` raises events that drive
state. This ADR takes that **programming model** but differs in two deliberate ways. First, `reduce`
is a **pure function returning new immutable state** — the React discipline — rather than a mutating
`Apply` / `TransitionState`, which keeps transitions side-effect-free and trivially testable. Second,
it persists a **snapshot** of the reduced state. Durable event-sourcing / journaling — keeping the
event log itself — is a *distinct persistence capability* (Orleans' log-consistency providers, and
Orleans 10's `Orleans.Journaling` / `DurableGrain`); this project addresses that separately as the
`DurableGrain` parity item on the [roadmap](../13-roadmap-and-phases.md), **not** as a persistence
mode of reducer grains. Reducer grains are the state-management model; durable journaling is its own
feature.

The model fits the virtual-actor runtime particularly well, because the actor removes the hardest
part — serializing state evolution against concurrent writers:

- **One activation per key** is the single writer for an aggregate; there is no contention.
- **One turn at a time** ([02](../02-actor-model.md)) makes the fold serialized and deterministic
  without locks; events apply in a total order per grain.
- The existing subsystems compose: persistence (snapshot via `GrainStorage`, etag ↔ expected
  version), streams (events are the obvious thing to publish for projections), reminders/timers (a
  tick becomes a command).

The question this ADR settles: should the runtime offer reducer grains as a first-class programming
model, and is it compatible? It is.

## Decision

Introduce reducer grains as an **additive programming model** layered on the runtime, not a
replacement for the mutable facet.

**The model** (a facet/base, e.g. `@reducer<S, E>({ initial, reduce })`):

- `this.state: Readonly<S>` — an immutable value, replaced wholesale, never mutated.
- **Command handlers** are ordinary grain-interface methods. They do the impure work (validate, read
  the clock, call other grains), decide on events, and `raise(event)`. `raise` folds the event
  through the pure `reduce` (replacing the immutable state reference) and collects it for the turn.
- The **reducer is pure**: no I/O, no grain calls, no clock — it only folds. Enforcing the
  impure-handler / pure-reducer split is the one discipline the API imposes.

**Persistence: snapshot.** Persist the folded `S` through the existing `GrainStorage` + etag; the
raised events are transient per-turn values, exactly as a React app keeps reduced state and discards
dispatched actions. This adds no new store. Where a durable record of *what happened* is wanted, the
raised events are published to a stream (below) — and a richer durable-journaling model, if ever
needed, is the separate `DurableGrain` path, not a reducer-specific event log.

Stream publication is opt-in and independent: raised events may also be published to the grain's
event stream (`getStreamProvider().getStream("events", this.id.key)`) so projections and sagas reuse
the stream layer ([09](../09-event-streams.md)) — and serve as the durable event history when one is
wanted.

The proxy/interface plumbing (`defineGrainInterface`, `Proxy` references — [ADR 0001](0001-runtime-proxy-grain-references.md))
is **orthogonal** to the reducer: unchanged, and a future declarative/generated grain layer would sit
*above* the reducer, generating boilerplate while the hand-written `reduce` stays beneath.

## Rationale

- **The reducer's value is independent of durability.** Pure, total-function state transitions are
  testable in isolation, make every state change explicit, and remove ad-hoc mutation — which is why
  React reaches for reducers without persisting any action log. Snapshot persistence is the natural
  pairing and is enough on its own.
- **The runtime's strongest guarantee does the hard part.** Single-threaded turns + single activation
  give a serialized, deterministic, lock-free fold — what the model needs and what is otherwise
  expensive to achieve.
- **It reuses existing machinery rather than adding a parallel stack.** `GrainStorage` + etag for
  snapshots, the serializer/value-codec for state and events, streams for projections and event
  history, reminders for scheduled commands.
- **Additive, so it does not destabilise the shipped model.** `@persistentState` grains and reducer
  grains coexist; a grain opts in.

## Consequences

- **Snapshot persistence is `GrainStorage`** — no new provider, so reducer grains stand alone on the
  shipped storage layer.
- **Reducers must be pure**, and **command handlers that `await`** (e.g. call another grain before
  raising an event) can observe intermediate state on a `@reentrant` or `readOnly`-interleaving grain
  ([02](../02-actor-model.md)). The model treats raising events as the synchronous tail of a handler
  and documents the reentrancy caveat; the default non-reentrant grain is safe.
- **State must be serializable** through the existing codec ([04](../04-messaging-and-serialization.md));
  immutable-update authoring pairs naturally with it. The same holds for events when they are
  published to a stream.

## Alternatives considered

1. **Mutable state facet only (status quo).** Simple and shipped, but mixes events with current value
   and scatters transitions across mutations; does not serve the reducer authoring model.
2. **Reducer bundled with a durable event log.** Conflating the programming model with event-sourcing
   persistence would force an event store on every reducer grain and pay full replay cost per
   activation. Scoping the reducer to snapshot persistence — and treating durable journaling as a
   separate capability (the `DurableGrain` path), with streams covering event history meanwhile —
   keeps reducer grains low-cost and avoids duplicating the journaling/streams stack.
3. **An external reducer/event-sourcing framework beside the runtime.** Would duplicate identity,
   single-writer and turn semantics the runtime already provides, and fight the actor model rather
   than build on it.
