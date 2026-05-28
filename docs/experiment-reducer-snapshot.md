# Experiment: React-app conventions in a virtual-actor runtime

> **Status: experiment.** A design exploration of why the runtime can credibly have **no code
> generation** — not because of a clever runtime trick, but because the authoring layer follows the
> conventions of a React/Redux application. See
> [`experiment-k8s-shard-layer.md`](experiment-k8s-shard-layer.md) for the complementary cluster
> experiment and [`deviations.md`](deviations.md) for what is true today.

## The thesis

The port has no code generation. The *mechanism* is well known and already documented as a deviation
(runtime `Proxy` references, runtime serializer registry, no build step). The *justification* — the
deeper reason this is sustainable rather than a constant fight against missing affordances — is that
the authoring layer adopts the conventions of a React application: **uniform function shapes, hooks,
immutable plain data, reference equality for change detection, and a single dispatch surface for
reducer grains**. Once those conventions are in place, there is nothing per-grain left to generate.

This experiment is about leaning into that fully and naming it as the design choice it is.

## What the reducer form actually is (so we don't conflate)

`reduce(state, action) → { state, effects }` produces a **new immutable snapshot** persisted through
the ordinary `GrainStorage` with **etag optimistic concurrency**; **the actions are transient — only
the reduced `value` is durable** (`packages/persistence/src/reducer-state-impl.ts`,
`packages/core/src/define-reducer-grain.ts`). This is **Redux, not event sourcing.** There is no log,
no replay; activation is O(1) (load the snapshot). The reducer is a *discipline over the existing
snapshot facet*, plus an `Effect`/`call` channel for cross-grain work returned as data.

This is the most extreme expression of the React convention — the entire public surface is
`dispatch(action)` + `query()` regardless of which grain or which actions — but the same convention
runs through `defineGrain` with named methods, too.

## What Orleans generates, and which React convention obviates each

| Orleans codegen produces | Convention that makes it unnecessary |
| --- | --- |
| A typed **proxy class per interface** (typed method calls → messages) | **Uniform function shape.** A grain is a factory closure; references are one runtime `Proxy` dispatching by method name; the TS interface is a compile-time view. For reducer grains the surface collapses further to `dispatch`/`query`, so there is no per-grain method table even in principle. |
| **Serializers per `[GenerateSerializer]` type** (field IDs, schema, copy methods) | **Immutable plain data** flowing through *one* serialize/deserialize path. Custom classes register once with `@serializable`; reference equality (`state !== previous` at `define-reducer-grain.ts:97`) handles dirty-tracking without generated per-field comparisons. |
| **Invokers / dispatch tables per interface** (message → method) | **One uniform invoke loop** dispatching by method name (or by the fixed `dispatch` surface for reducer grains). The action discriminated union carries the per-grain dispatch shape at the *type* level, where TypeScript already lives — not at the runtime level, where it would have to be materialized. |
| **Activators / DI factories per grain** | **Hooks + factory closures.** `usePersistentState`, `useReducerState`, `useDurable*` read from an ambient "currently-running setup" context — the same trick React uses for `useState`. No per-grain DI registration, no metadata to generate; the convention (rules of hooks, hook ordering) establishes the contract. |

The pattern is the same in every row: **the variability is moved from runtime shape into types and
conventions**. Orleans needs codegen because C# does not give you cheap dynamic dispatch, first-class
closures-as-grains, an ambient hook context, or a runtime serializer registry that competes with
generated code for speed. Take those affordances and the React conventions that exploit them, and the
codegen falls away — *not* as a hack, but because there is genuinely nothing for it to do.

## What this does *not* delete

- **`Journaling` / `EventSourcing` are not displaced.** They deliver a different capability —
  *durable event history*, audit, log views, projections from history — that the snapshot model does
  not. A reducer-over-snapshot grain that also wants an event log keeps the log facet; the two
  compose. Treating them as redundant was a mistake.
- **ACID transactions remain the right tool for true cross-grain invariants.** Effects-as-data are a
  Redux/Elm-style process-manager substrate already present in the runtime, so workflow-style
  coordination is first-class — but that is an *additional* option, not a replacement for the
  transaction facet.
- **The mechanism deviations stand.** Runtime `Proxy`, runtime serializer registry, no build step
  (documented in [`deviations.md`](deviations.md)) are the *how*. This document is the *why*.

## What it enables beyond no-codegen

- **Browser replication is Redux, literally.** Snapshot + reducer + transient actions is the Redux
  model — not an analogy. Ship the browser the snapshot, stream actions, fold with the *same pure
  reducer*; reference equality drives React change detection on the client. The reducer convention is
  the direct substrate for the browser-state-replication goal.
- **Hydration as the migration model.** Serializing a snapshot and rehydrating it on a different pod
  is structurally the same as SSR hydration in a Redux app; migration becomes a familiar pattern.
- **Devtools-shaped inspection is natural.** Action stream + immutable snapshots is exactly what
  Redux DevTools consumes; a grain inspector can mirror that shape.
- **Etag CAS at the grain layer pairs with `resourceVersion` CAS at the cluster layer.** Same
  compare-and-swap primitive; under the shard layer in
  [`experiment-k8s-shard-layer.md`](experiment-k8s-shard-layer.md), one pod is the single writer of a
  shard's grains, so the snapshot write is lock-free by construction.

## Honest costs

- **The pure-fold constraint is real.** Not every grain is a clean total fold; large-binary or
  I/O-heavy grains are awkward as snapshot-by-replacement. The class and closure forms remain as the
  escape hatch — this is reducer-*first*, not reducer-*only*.
- **The React convention is opinionated.** Hooks have rules; immutability requires care; reference
  equality only works if you actually return new objects. The convention is what buys the no-codegen
  property, and breaking it locally reintroduces the friction codegen would normally absorb.
- **No generated fast path.** Orleans' generated serializers are fast on purpose. A runtime registry
  with MessagePack is fast enough for the workloads the port targets, but a hot path that *needs*
  generated read/write code is a workload this convention does not optimise for.
