// Test harness for the GAP-TRACING propagation ports: captures finished spans
// in memory (mirroring upstream's `ActivityListener` bagging every started
// `Activity` into a `ConcurrentBag`) and lets a test run code inside a "test
// parent" span, the analogue of upstream's
// `ActivitySources.ApplicationGrainSource.StartActivity("client-parent-activity")`.
//
// Registers a real OpenTelemetry SDK — `BasicTracerProvider` +
// `SimpleSpanProcessor(InMemorySpanExporter)` for capture, and
// `AsyncLocalStorageContextManager` so span context (parent/child) survives
// `await` boundaries across grain calls — plus the W3C propagator so
// `tracingFilters()` (see `@tsva/observability/tracing`) actually injects and
// extracts `traceparent`/`tracestate`.
import {
  context,
  propagation,
  trace,
  type Span,
  type SpanKind,
} from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { TraceState, W3CTraceContextPropagator } from "@opentelemetry/core";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
} from "@opentelemetry/sdk-trace-base";
import { setupTracePropagation } from "@tsva/observability/tracing";

export interface CapturedSpan {
  readonly name: string;
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId: string | undefined;
  readonly parentIsRemote: boolean;
  readonly kind: SpanKind;
  readonly traceState: string | undefined;
  readonly attributes: Readonly<Record<string, unknown>>;
}

function toCaptured(span: ReadableSpan): CapturedSpan {
  const parentSpanContext = span.parentSpanContext;
  return {
    name: span.name,
    traceId: span.spanContext().traceId,
    spanId: span.spanContext().spanId,
    parentSpanId: parentSpanContext?.spanId,
    parentIsRemote: parentSpanContext?.isRemote === true,
    kind: span.kind,
    traceState: span.spanContext().traceState?.serialize(),
    attributes: span.attributes,
  };
}

/**
 * Registers a global tracer provider (in-memory capture), the async-hooks
 * context manager, and W3C propagation. Call once per test file (module
 * scope, like `packages/hosting/src/tracing.test.ts` does) — `reset()`
 * clears captured spans between tests, `teardown()` unregisters the globals
 * so they don't leak into other test files.
 */
export function createTracingHarness(): {
  /** Captured spans across every call since the last `reset()`, start order. */
  finishedSpans: () => CapturedSpan[];
  /** Clear captured spans; call in `beforeEach`. */
  reset: () => void;
  /**
   * Run `fn` inside a fresh root "test parent" span (the analogue of
   * upstream's `ActivitySources.ApplicationGrainSource.StartActivity(name)`),
   * ending the span afterwards. Returns `fn`'s result and the parent span's
   * own trace/span ids for assertions.
   */
  withParentSpan: <T>(
    name: string,
    fn: () => Promise<T>,
  ) => Promise<{ result: T; traceId: string; spanId: string }>;
  /** Run `fn` with no active span (Activity.Current == null upstream). */
  withNoActiveSpan: <T>(fn: () => Promise<T>) => Promise<T>;
  /**
   * Run `fn` inside a parent span whose `tracestate` is `traceStateStr`
   * (upstream: `clientActivity.TraceStateString = "vendor1=value1,..."`).
   * `tracestate` is carried on the *SpanContext*, not settable on an
   * already-started span via the public API, so this fabricates a parent
   * context bearing the desired trace state and starts the span as its
   * child — the same shape a real remote parent with trace state would take.
   */
  withTraceState: <T>(
    name: string,
    traceStateStr: string,
    fn: () => Promise<T>,
  ) => Promise<{ result: T; traceId: string }>;
  teardown: () => Promise<void>;
} {
  const exporter = new InMemorySpanExporter();
  const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
  const previousTracerProvider = trace.getTracerProvider();
  const contextManager = new AsyncLocalStorageContextManager().enable();
  trace.setGlobalTracerProvider(provider);
  context.setGlobalContextManager(contextManager);
  setupTracePropagation();

  const tracer = trace.getTracer("@tsva/parity/trace-context-propagation");

  return {
    finishedSpans: () => exporter.getFinishedSpans().map(toCaptured),
    reset: () => exporter.reset(),
    withParentSpan: async <T>(name: string, fn: () => Promise<T>) => {
      let result!: T;
      let traceId!: string;
      let spanId!: string;
      await tracer.startActiveSpan(name, async (span: Span) => {
        const sc = span.spanContext();
        traceId = sc.traceId;
        spanId = sc.spanId;
        try {
          result = await fn();
        } finally {
          span.end();
        }
      });
      return { result, traceId, spanId };
    },
    withNoActiveSpan: <T>(fn: () => Promise<T>) =>
      context.with(trace.deleteSpan(context.active()), fn),
    withTraceState: async <T>(name: string, traceStateStr: string, fn: () => Promise<T>) => {
      let result!: T;
      let traceId!: string;
      await tracer.startActiveSpan(name, async (parentSpan: Span) => {
        const sc = parentSpan.spanContext();
        traceId = sc.traceId;
        const stateful = { ...sc, traceState: new TraceState(traceStateStr) };
        await context.with(trace.setSpanContext(context.active(), stateful), async () => {
          try {
            result = await fn();
          } finally {
            parentSpan.end();
          }
        });
      });
      return { result, traceId };
    },
    teardown: async () => {
      await provider.shutdown();
      context.disable();
      trace.disable();
      if (previousTracerProvider !== undefined) trace.setGlobalTracerProvider(previousTracerProvider);
      propagation.disable();
    },
  };
}
