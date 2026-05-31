import {
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  trace,
  type Attributes,
  type Span,
} from "@opentelemetry/api";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import type {
  GrainCallContext,
  IncomingGrainCallFilter,
  OutgoingGrainCallFilter,
} from "@tsva/core/grain-call-filter";

const tracer = trace.getTracer("@tsva/observability");

const spanName = (ctx: GrainCallContext): string => `${ctx.interfaceName}/${ctx.methodName}`;

function attributes(ctx: GrainCallContext): Attributes {
  return {
    "rpc.system": "tsva",
    "rpc.service": ctx.interfaceName,
    "rpc.method": ctx.methodName,
    "tsva.grain_id": ctx.target.toString(),
  };
}

/** Run `body` inside a new span, recording the outcome and ending it. */
async function withSpan(
  name: string,
  kind: SpanKind,
  ctx: GrainCallContext,
  body: () => Promise<void>,
): Promise<void> {
  await tracer.startActiveSpan(name, { kind, attributes: attributes(ctx) }, async (span: Span) => {
    try {
      await body();
      span.setStatus({ code: SpanStatusCode.OK });
    } catch (err) {
      span.recordException(err as Error);
      span.setStatus({
        code: SpanStatusCode.ERROR,
        message: err instanceof Error ? err.message : String(err),
      });
      throw err;
    } finally {
      span.end();
    }
  });
}

/**
 * Ensure W3C trace context is the propagation format, so trace ids ride the
 * request-context headers across grain calls and silos. Idempotent; the user's
 * OpenTelemetry SDK may set its own propagator instead.
 */
export function setupTracePropagation(): void {
  propagation.setGlobalPropagator(new W3CTraceContextPropagator());
}

/**
 * Grain call filters that trace calls with OpenTelemetry (the analogue of
 * Orleans' `ActivityPropagationGrainCallFilter`): the outgoing filter opens a
 * CLIENT span and injects W3C trace context into the request-context headers;
 * the incoming filter extracts the parent and opens a SERVER span, so spans
 * stitch across grain calls and silos. Spans are emitted through the global
 * OpenTelemetry tracer — a no-op until the host registers an SDK.
 */
export function tracingFilters(): {
  incoming: IncomingGrainCallFilter;
  outgoing: OutgoingGrainCallFilter;
} {
  const outgoing: OutgoingGrainCallFilter = async (ctx) =>
    withSpan(spanName(ctx), SpanKind.CLIENT, ctx, async () => {
      propagation.inject(context.active(), ctx.headers);
      await ctx.invoke();
    });

  const incoming: IncomingGrainCallFilter = async (ctx) => {
    const parent = propagation.extract(context.active(), ctx.headers);
    await context.with(parent, () =>
      withSpan(spanName(ctx), SpanKind.SERVER, ctx, () => ctx.invoke()),
    );
  };

  return { incoming, outgoing };
}
