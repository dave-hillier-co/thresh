# ADR 0006 — Reducer grains (event-routed, immutable state)

- Status: Proposed
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
in its own right, not a synonym for event sourcing. React/Redux uses exactly this shape with
`useReducer`: actions are routed through a pure reducer to produce new immutable state, and the
actions are **transient** — they are not stored; what you keep (if anything) is a snapshot of the
reduced state. Event sourcing is the *other* persistence choice on top of the same reducer: keep the
events and derive the state by replay. So the reducer is the programming model; **how much you
persist is orthogonal.**

Orleans is direct prior art: `JournaledGrain<TState, TEvent>` raises events that drive state, and its
**log-consistency providers make exactly this orthogonal split** — `LogStorage` persists the events
(event sourcing), `StateStorage` persists only the latest snapshot and discards the events, and
`CustomStorage` lets you supply your own. This ADR adopts the same separation. It differs in one
deliberate way: `JournaledGrain` conventionally *mutates* state in an `Apply` / `TransitionState`
method, whereas here `reduce` is a **pure function returning new immutable state** — the React
discipline — which keeps transitions side-effect-free and trivially testable.

The model fits the virtual-actor runtime particularly well, because the actor removes the hardest
part — serializing state evolution against concurrent writers:

- **One activation per key** is the single writer for an aggregate; there is no contention.
- **One turn at a time** ([02](../02-actor-model.md)) makes the fold serialized and deterministic
  without locks; events apply in a total order per grain.
- The existing subsystems compose: persistence (snapshot via `GrainStorage`, etag ↔ expected
  version), streams (events are the obvious thing to publish for projections), reminders/timers (a
  tick becomes a command).

The question this ADR settles: should the runtime offer reducer grains as a first-class programming
model, and is it compatible? It is — and it serves both the snapshot and the event-sourced ends.

## Decision

Introduce reducer grains as an **additive programming model** layered on the runtime, not a
replacement for the mutable facet. The model and its persistence are separate concerns.

**The model** (a facet/base, e.g. `@reducer<S, E>({ initial, reduce })`):

- `this.state: Readonly<S>` — an immutable value, replaced wholesale, never mutated.
- **Command handlers** are ordinary grain-interface methods. They do the impure work (validate, read
  the clock, call other grains), decide on events, and `raise(event)`. `raise` folds the event
  through the pure `reduce` (replacing the immutable state reference) and collects it for the turn.
- The **reducer is pure**: no I/O, no grain calls, no clock — it only folds. Enforcing the
  impure-handler / pure-reducer split is the one discipline the API imposes.

**Persistence is orthogonal — two modes, both first-class:**

- **Snapshot (default).** Persist the folded `S` through the existing `GrainStorage` + etag; the
  raised events are transient per-turn values, exactly as a React app keeps reduced state and
  discards dispatched actions. This is a complete, sufficient choice — most grains will use it — and
  it adds no new store.
- **Event log.** Persist the events through an append-only `EventLog` provider (true event sourcing);
  derive state by replaying from a snapshot's tail on activation. Adds audit, time-travel, and
  projection rebuild, at the cost of an event store and schema-evolution handling.

A grain picks a mode; the reducer authoring is identical either way. Stream publication is opt-in and
independent of both: raised events may also be published to the grain's event stream
(`getStreamProvider().getStream("events", this.id.key)`) so projections and sagas reuse the stream
layer ([09](../09-event-streams.md)).

The proxy/interface plumbing (`defineGrainInterface`, `Proxy` references — [ADR 0001](0001-runtime-proxy-grain-references.md))
is **orthogonal** to the reducer: unchanged, and a future declarative/generated grain layer would sit
*above* the reducer, generating boilerplate while the hand-written `reduce` stays beneath.

## Rationale

- **The reducer's value is independent of durability.** Pure, total-function state transitions are
  testable in isolation, make every state change explicit, and remove ad-hoc mutation — which is why
  React reaches for reducers without persisting any action log. Snapshot persistence is the natural
  pairing and is enough on its own.
- **The runtime's strongest guarantee does the hard part.** Single-threaded turns + single activation
  give a serialized, deterministic, lock-free fold — what both modes need and what is otherwise
  expensive to achieve.
- **It reuses existing machinery rather than adding a parallel stack.** `GrainStorage` + etag for
  snapshots, the serializer/value-codec for state (and events), streams for projections, reminders
  for scheduled commands.
- **One model spans both ends of the spectrum.** The same `reduce` serves an in-memory/snapshot grain
  and an event-sourced grain; teams move along the spectrum by changing the persistence mode, not the
  domain code.
- **Additive, so it does not destabilise the shipped model.** `@persistentState` grains and reducer
  grains coexist; a grain opts in.

## Consequences

- **Snapshot mode needs no new provider** (it is `GrainStorage`), so it can ship first and stand
  alone. The **`EventLog` provider contract is future work** — append-only, append-at-version, range
  read for replay — plus a snapshotting policy (every N events / on deactivation).
- **Reducers must be pure**, and **command handlers that `await`** (e.g. call another grain before
  raising an event) can observe intermediate state on a `@reentrant` or `readOnly`-interleaving grain
  ([02](../02-actor-model.md)). The model treats raising events as the synchronous tail of a handler
  and documents the reentrancy caveat; the default non-reentrant grain is safe.
- **Event schema evolution (upcasting)** is a concern **only in event-log mode**; snapshot mode is
  unaffected because events are transient. It is the same class of problem as grain-interface
  versioning, already a deferred non-goal ([01](../01-overview-and-goals.md)), so it is deferred too.
- **State (and, in event-log mode, events) must be serializable** through the existing codec
  ([04](../04-messaging-and-serialization.md)); immutable-update authoring pairs naturally with it.
- **Provider conformance testing** extends to the `EventLog` once it lands, per
  [12](../12-project-structure-and-tooling.md).

## Alternatives considered

1. **Mutable state facet only (status quo).** Simple and shipped, but mixes events with current value
   and scatters transitions across mutations; does not serve the reducer authoring model.
2. **Reducer tied to event sourcing (durable log required).** Conflates the programming model with one
   persistence choice, forces an event store from day one, and pays full replay cost per activation.
   Treating persistence as orthogonal keeps snapshot mode as a complete, low-cost default and makes
   the log an additive capability.
3. **An external reducer/event-sourcing framework beside the runtime.** Would duplicate identity,
   single-writer and turn semantics the runtime already provides, and fight the actor model rather
   than build on it.
