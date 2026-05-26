# ADR 0013 — Observability (request context + OpenTelemetry tracing)

- Status: Accepted — request context, tracing, and metrics implemented; structured logs to follow
- Context docs: [04 — Messaging and serialization](../04-messaging-and-serialization.md),
  [13 — Roadmap](../13-roadmap-and-phases.md), [ADR 0012 — Grain call filters](0012-grain-call-filters.md)

> Orleans references: `Orleans.Core/Diagnostics/ActivityPropagationGrainCallFilter.cs` (tracing as a
> call filter), `Orleans.Runtime/RequestContext.cs` (ambient request context).

## Context

Orleans-10 parity ([13](../13-roadmap-and-phases.md)) calls for cross-cutting observability —
distributed traces, metrics, and structured logs that stitch across grain calls and silos. Orleans
implements tracing as a **grain call filter** that flows W3C trace context through its ambient
**RequestContext**. Both primitives now exist here: the call-filter seam ([ADR 0012](0012-grain-call-filters.md))
and request-context propagation. This ADR records how observability builds on them.

## Decision

1. **Ambient request context.** `requestContext.get/set/getAll` over a string→string header bag that
   flows along the call chain — in-process via the `AsyncLocalStorage` invocation context, and across
   silos via the message envelope's `requestContext.headers`. The proxy copies the ambient headers
   onto each outgoing request; the activation seeds a fresh, mutable copy per turn. This is Orleans'
   `RequestContext`: the carrier for W3C trace context and application baggage (tenant, correlation
   id). The `GrainCallContext` also exposes `headers` so a filter can inject/extract per call.

2. **Tracing via OpenTelemetry, as call filters.** A new `@tsva/observability` package depends on
   `@opentelemetry/api` — the TypeScript analogue of .NET's `System.Diagnostics.Activity` that Orleans
   traces with. `tracingFilters()` returns an outgoing filter (opens a `CLIENT` span, injects W3C
   trace context into the outgoing headers) and an incoming filter (extracts the parent from the
   arrived headers, opens a `SERVER` span as its child); both record exceptions and status. Spans go
   through the **global** OpenTelemetry tracer, so they are a **no-op until the host registers an OTel
   SDK** — zero overhead by default, full traces when wired. `createSilo().useTracing()` registers the
   filters and sets W3C propagation.

3. **Metrics via OpenTelemetry, as a call filter.** `metricsFilters()` returns an incoming filter
   recording a `tsva.grain.calls` counter (by interface, method, ok/error status) and a
   `tsva.grain.call.duration` histogram, through the global OpenTelemetry meter (no-op without an
   SDK). `createSilo().useMetrics()` registers it plus a `tsva.activations` observable gauge
   sampling the live activation count from the catalog. The directory-hit-rate / reminder-stream-lag
   gauges and **structured logs** follow, on the same seam and instrumentation points.

## Consequences

- The only runtime dependency is `@opentelemetry/api` (+ `@opentelemetry/core` for the W3C
  propagator) in `@tsva/observability`; the core runtime stays OTel-free. Hosts opt in via
  `useTracing()` and bring their own SDK/exporter.
- Trace propagation is **explicit** (inject/extract over the request-context headers), so it works
  uniformly for local and cross-silo calls and does not depend on the OTel context manager surviving
  the turn scheduler.
- Application code can read/write request-context headers for its own baggage, independent of tracing.

## Alternatives considered

- **A project-defined tracer abstraction instead of OpenTelemetry.** Would avoid the dependency but
  reinvent spans/propagation; OTel is the ecosystem standard and the faithful analogue of Orleans'
  Activity-based tracing. Rejected.
- **Tracing baked into the dispatcher.** Hard-codes the concern and offers no opt-out; the call-filter
  seam ([ADR 0012](0012-grain-call-filters.md)) is the composable, Orleans-faithful place.
