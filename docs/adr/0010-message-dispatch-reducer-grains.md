# ADR 0010 — Message-dispatch reducer grains (no per-grain interface, no codegen)

- Status: Accepted — `defineReducerGrain` (`dispatch` / `query` + Elm-style effects) shipped, with an
  `examples/bank` reducer account and an e2e test. Builds on [ADR 0009](0009-functional-grains.md) as
  its zero-boilerplate specialization. Only the `send` effect ships so far.
- Context docs: [02](../02-actor-model.md), [07](../07-persistence.md),
  [ADR 0006](0006-reducer-grains.md), [ADR 0009](0009-functional-grains.md)

## Context

[ADR 0001](0001-runtime-proxy-grain-references.md) removed compile-time *proxy* generation with a
runtime `Proxy`, but a grain still hand-writes a **method table** (`defineGrainInterface` with an
ordered `methods` list) that duplicates its TypeScript interface — exactly what an Orleans-style
generator produces, now in hand-maintained strings that must track the interface, where method *order*
assigns the wire id (a careless reorder silently renumbers the protocol). React's `useReducer` and the
Erlang/Akka tradition point the other way: an actor receives **messages**, not RPC calls. A
discriminated-union `Action` plus one `dispatch` channel carries the same information type-safely with
**nothing to generate** — the `Action` type *is* the protocol.

## Decision

Offer `defineReducerGrain<S, A>(name, { initial, reduce })` whose entire wire surface is **two fixed
methods, identical for every reducer grain**:

- `dispatch(action: A): Promise<S>` — the write channel; folds the action and returns the new state.
- `query(): Promise<S>` — a read-only snapshot.

So there is **no per-grain method table**; the `<S, A>` generics give the caller full type safety
(`getGrain(Account, key).dispatch({ type: "deposit", cents })`). The reducer is **pure and effects are
data** (Elm/Redux): `reduce(state, action) => { state, effects? }`, where cross-grain calls and I/O are
returned as **effect descriptors** (`call(grain, key, action)`) the runtime runs *after* folding and
persisting the snapshot — keeping the reducer side-effect-free and testable
([ADR 0006](0006-reducer-grains.md)). Snapshot persistence reuses `GrainStorage`; events/effects are
transient. It is **additive and layered** on `defineGrain` ([ADR 0009](0009-functional-grains.md)) — a
dispatch grain is just a functional grain returning `{ dispatch, query }` over a `usePersistentState`
snapshot, routed by the ordinary `Proxy`/dispatcher with no runtime change.

The actor runtime already supplies what a dispatch loop needs: one activation per key (single writer),
one turn at a time (a serialized deterministic fold), and at-least-once effect delivery through grain
calls.

## Consequences

- **Trades N typed RPC methods for one `dispatch` + a union.** Per-method options
  (`readOnly`/`alwaysInterleave`/`oneWay`) no longer apply per command — reads use the fixed read-only
  `query`, writes share one exclusive `dispatch`; per-action interleaving or one-way sends would need
  action-level options or a second channel (deferred).
- **Effects are an open set; only `send` ships.** Timers, reminders, stream publication and
  self-dispatch would be added as effect kinds (an injected interpreter would make them testable
  without a live runtime).
- **Effects run after the snapshot write**, so a transfer stays two non-atomic steps — not a
  distributed transaction. Rejections are signalled by the reducer throwing (deterministic, no state
  change).
- **An idiomatic-TS authoring shape, not a semantic divergence.** Orleans' `JournaledGrain` is
  class-based; this expresses the same event-routed model in the `useReducer` idiom with identical
  runtime semantics (single activation, single-turn fold, snapshot persistence). The class +
  `defineGrainInterface` model is retained as the substrate/interop surface.

## Alternatives considered

- **Code generation** to derive the method table — removes the duplication but adds a build step,
  artifacts, and a tool; the dispatch model removes it with none of that.
- **Stop at multi-method functional grains ([ADR 0009](0009-functional-grains.md))** — ergonomic, but
  each grain still needs its `defineGrainInterface` table, so it doesn't reach "no codegen".
- **Imperative awaits in an impure handler (no pure reducer)** — simplest, but discards the isolated,
  testable fold that is the reason to reach for `useReducer`.
