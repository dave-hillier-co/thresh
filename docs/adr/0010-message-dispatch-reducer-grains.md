# ADR 0010 — Message-dispatch reducer grains (no per-grain interface, no codegen)

- Status: Proposed — spike (`defineReducerGrain` with `dispatch` / `query` + Elm-style effects, an
  `examples/bank` `account-reducer-grain`, and an end-to-end test). Builds on
  [ADR 0009](0009-functional-grains.md). The class + `defineGrainInterface` model remains the
  default.
- Context docs: [02 — The actor model](../02-actor-model.md),
  [07 — Persistence](../07-persistence.md), [ADR 0001](0001-runtime-proxy-grain-references.md),
  [ADR 0006](0006-reducer-grains.md), [ADR 0009](0009-functional-grains.md)

## Context

[ADR 0001](0001-runtime-proxy-grain-references.md) removed compile-time *proxy* generation with a
runtime `Proxy`. But a grain still hand-writes a **method table** that duplicates its TypeScript
interface:

```ts
export interface IAccount extends GrainWithStringKey {
  deposit(cents: number): Promise<number>;
  withdraw(cents: number): Promise<number>;
  transferTo(other: string, cents: number): Promise<number>;
  statement(): Promise<AccountState>;
}
export const IAccount = defineGrainInterface<IAccount>("example.bank.IAccount", {
  methods: ["deposit", "withdraw", "transferTo", "statement"], // index = wire id
  options: { statement: { readOnly: true } },
});
```

That table is exactly what an Orleans-style code generator produces; the cost didn't disappear, it
moved into hand-maintained strings that must stay in lockstep with the interface — and because method
*order* assigns the wire id, a careless reorder silently renumbers the protocol.

React's `useReducer` and the older actor tradition (Erlang/Akka) point at the other shape: an actor
receives **messages**, not RPC calls. A discriminated-union `Action` plus a single `dispatch` channel
carries the same information with full type safety and **nothing to generate** — the `Action` type
*is* the protocol.

## Decision

Offer `defineReducerGrain<S, A>(name, { initial, reduce })`, whose entire wire surface is **two fixed
methods, identical for every reducer grain**:

- `dispatch(action: A): Promise<S>` — the write channel; folds the action and returns the new state.
- `query(): Promise<S>` — a read-only snapshot (interleaves, no write).

Because the surface is the same two methods for every reducer grain, there is **no per-grain method
table** to write or generate; the `<S, A>` generics give the caller full type safety
(`getGrain(Account, key).dispatch({ type: "deposit", cents })`).

**The reducer is pure and effects are data** (the Elm / Redux shape):
`reduce(state, action) => { state, effects? }`. Cross-grain calls and other I/O are returned as
**effect descriptors** (`call(grain, key, action)`) that the runtime runs *after* folding and
persisting the snapshot — keeping the reducer side-effect-free and testable in isolation, consistent
with [ADR 0006](0006-reducer-grains.md)'s pure-fold discipline. Snapshot persistence reuses
`GrainStorage`; events and effects are transient.

It is **additive and layered**: `defineReducerGrain` is built on `defineGrain`
([ADR 0009](0009-functional-grains.md)) — the dispatch grain is just a functional grain returning
`{ dispatch, query }` over a `usePersistentState` snapshot. No runtime change: under the hood it is an
ordinary two-method interface the existing `Proxy` and dispatcher route.

## Rationale

- **It removes the last piece of per-grain boilerplate** — the method table — and the class of bug it
  carries (method order = wire id), with no build step, no generated artifacts and no codegen tool to
  maintain.
- **Pure reducer + effects-as-data is the faithful `useReducer` port and the most testable shape:**
  state transitions are a pure function; effects are inspectable values, not hidden awaits.
- **The actor runtime already supplies what a dispatch loop needs** — one activation per key (a single
  writer), one turn at a time (a serialized, deterministic fold), and at-least-once delivery for
  effects through ordinary grain calls.
- **Layering on [ADR 0009](0009-functional-grains.md) keeps the surface tiny and shows the hierarchy:**
  `defineGrain` is the general functional primitive; `defineReducerGrain` is its zero-boilerplate
  specialization.

## Consequences

- **It trades N typed RPC methods for one `dispatch` + a discriminated union.** Per-method invocation
  options (`readOnly` / `alwaysInterleave` / `oneWay` — [02](../02-actor-model.md)) no longer apply
  per command: reads go through the fixed read-only `query`, and all writes share one exclusive
  `dispatch`. Per-action interleaving or one-way sends would need action-level options or a second
  channel — deferred.
- **Effects are an open set; the spike ships only `send`** (dispatch to another grain). Timers,
  reminders, stream publication and self-dispatch would be added as effect kinds the runtime knows how
  to run; an injected effect interpreter would make them testable without a live runtime.
- **Effects run after the snapshot write**, so a transfer stays two non-atomic steps (debit durable
  before credit) — exactly as the existing bank grain documents, not a distributed transaction.
- **Rejections are signalled by the reducer throwing** (deterministic, no state change). Modelling
  failures as state/events instead is a style choice left to the author.
- **It is a further idiomatic-TS authoring shape, not a semantic divergence.** Orleans'
  `JournaledGrain` is class-based; this expresses the same event-routed model in the `useReducer`
  idiom. The runtime semantics — single activation, single-turn deterministic fold, snapshot
  persistence — are identical, so Orleans parity holds; what changes is how the grain is written.
  Like [ADR 0009](0009-functional-grains.md) it stays additive and opt-in, and the class +
  `defineGrainInterface` model remains the default and only documented surface.

## Alternatives considered

1. **Code generation (a TS transformer / build plugin) to derive the method table from the
   interface.** Removes the duplication, but adds a build step, generated artifacts and a tool to
   maintain — the dispatch model removes the same duplication with none of that.
2. **Stop at multi-method functional grains ([ADR 0009](0009-functional-grains.md)).** Ergonomic, but
   each grain still needs its `defineGrainInterface` table, so it doesn't reach "no codegen".
3. **Effects as imperative awaits inside an impure dispatch handler (no pure reducer).** Simplest, but
   it discards the isolated, testable pure fold that is the whole reason to reach for `useReducer`.
