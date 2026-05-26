# ADR 0012 — Grain call filters

- Status: Accepted — incoming filters implemented; outgoing filters + per-grain filters to follow
- Context docs: [02 — The actor model](../02-actor-model.md),
  [04 — Messaging and serialization](../04-messaging-and-serialization.md),
  [13 — Roadmap](../13-roadmap-and-phases.md)

> Orleans references: `Orleans.Core.Abstractions/Core/IGrainCallFilter.cs`
> (`IIncomingGrainCallFilter` / `IOutgoingGrainCallFilter`),
> `Orleans.Core.Abstractions/Core/IGrainCallContext.cs`,
> `Orleans.Core/Diagnostics/ActivityPropagationGrainCallFilter.cs`.

## Context

Orleans-10 parity needs **grain call filters** ([13](../13-roadmap-and-phases.md)): interception
around grain calls for cross-cutting concerns — authorization, retries, validation, and especially
**trace/metric propagation**. Orleans exposes `IIncomingGrainCallFilter` (grain side) and
`IOutgoingGrainCallFilter` (caller side); each filter receives an `IGrainCallContext` and calls
`context.Invoke()` to proceed, forming a pipeline. It is the seam the observability work plugs into
(Orleans' own tracing is an `ActivityPropagationGrainCallFilter`).

## Decision

Mirror the Orleans model with a small functional surface.

- **`GrainCallContext`** carries the call: `target` / `source` grain ids, `interfaceId` /
  `interfaceName` / `methodName`, mutable `args` (a filter may rewrite arguments), mutable `result`
  (readable after proceeding, replaceable), and `invoke()` to continue. An `IncomingGrainCallContext`
  also exposes the activation's `grain` instance.
- **A filter is a function** `(context) => Promise<void>` (Orleans' delegate form): inspect/rewrite
  `args`, call `await context.invoke()` to proceed, then read/replace `result`. Omitting `invoke()`
  short-circuits the call (e.g. an auth denial or a cache hit).
- **`runCallFilters(filters, context, terminal)`** runs the chain by a shared cursor, then the
  terminal step (the grain method for incoming, the dispatch for outgoing), and returns the result.
- **Incoming filters** are silo-wide, registered on the host builder
  (`addIncomingCallFilter`), threaded through the catalog to each activation, and wrap the
  grain-method dispatch in `callMethod`. System extensions (stream delivery, transaction-resource,
  reminders) bypass the pipeline — they are infrastructure, not application grain calls.
- **Outgoing filters** (caller side, at the proxy) and **per-grain incoming filters** (a grain that
  filters its own calls, as in Orleans) are follow-on slices.

## Consequences

- The filter pipeline is the **single seam for observability** (W3C trace context over the request
  context, metrics, structured logs) — the analogue of Orleans' `ActivityPropagationGrainCallFilter`
  — and for auth/retry/validation, without scattering those concerns through grain code.
- Filters run **inside the activation turn** for incoming calls, so they observe the same
  single-threaded ordering as the method; an `await` in a filter is a turn-level await like any other.
- Rewriting `args` / `result` is deliberately allowed (Orleans parity); filters are trusted host
  configuration.

## Alternatives considered

- **Bake cross-cutting concerns into the dispatcher.** Hard-codes policy and gives applications no
  extension point; the filter pipeline is the Orleans-faithful, composable seam.
- **Decorator-only (per-method attributes).** Useful sugar later, but the pipeline is the primitive;
  attributes can register filters on top.
