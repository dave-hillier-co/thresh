import { afterAll, beforeEach, describe, expect, it } from "vitest";
import { context, propagation, SpanKind, trace } from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
} from "@opentelemetry/sdk-trace-base";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

// Register a minimal OpenTelemetry SDK so spans are recorded and trace context
// propagates (an in-memory exporter + an async-hooks context manager + W3C).
const exporter = new InMemorySpanExporter();
const provider = new BasicTracerProvider({ spanProcessors: [new SimpleSpanProcessor(exporter)] });
trace.setGlobalTracerProvider(provider);
context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
propagation.setGlobalPropagator(new W3CTraceContextPropagator());

interface Down extends GrainWithStringKey {
  ping(): Promise<string>;
  fail(): Promise<void>;
}
interface Front extends GrainWithStringKey {
  call(down: string): Promise<string>;
}
const Down = defineGrainInterface<Down>("TraceDown");
const Front = defineGrainInterface<Front>("TraceFront");

@grain({ name: "TraceDown" })
class DownGrain extends Grain implements Down {
  async ping(): Promise<string> {
    return "pong";
  }
  async fail(): Promise<void> {
    throw new Error("boom");
  }
}

@grain({ name: "TraceFront" })
class FrontGrain extends Grain implements Front {
  async call(down: string): Promise<string> {
    return this.getGrain(Down, down).ping();
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");
const parentSpanId = (s: ReadableSpan): string | undefined =>
  s.parentSpanContext?.spanId ?? (s as unknown as { parentSpanId?: string }).parentSpanId;

describe("OpenTelemetry tracing filter", () => {
  beforeEach(() => exporter.reset());
  afterAll(async () => {
    await provider.shutdown();
  });

  it("traces a grain-to-grain call as parent/child spans on one trace", async () => {
    const silo = createSilo({ clusterId: "trace", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useTracing()
      .registerGrain(FrontGrain, { interfaces: [Front] })
      .registerGrain(DownGrain, { interfaces: [Down] })
      .build();
    await silo.start();
    try {
      expect(await silo.getGrain(Front, "f").call("d")).toBe("pong");

      const spans = exporter.getFinishedSpans();
      // One trace across the whole call tree.
      expect(new Set(spans.map((s) => s.spanContext().traceId)).size).toBe(1);

      const downServer = spans.find(
        (s) => s.name === "TraceDown/ping" && s.kind === SpanKind.SERVER,
      );
      const downClient = spans.find(
        (s) => s.name === "TraceDown/ping" && s.kind === SpanKind.CLIENT,
      );
      expect(downServer).toBeDefined();
      expect(downClient).toBeDefined();
      // The callee's SERVER span is a child of the caller's CLIENT span.
      expect(parentSpanId(downServer!)).toBe(downClient!.spanContext().spanId);
      expect(downServer!.attributes["rpc.system"]).toBe("thresh");
      expect(downServer!.attributes["rpc.method"]).toBe("ping");
    } finally {
      await silo.stop();
    }
  });

  it("records an error status on the span when the method throws", async () => {
    const silo = createSilo({ clusterId: "trace-err", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useTracing()
      .registerGrain(DownGrain, { interfaces: [Down] })
      .build();
    await silo.start();
    try {
      await expect(silo.getGrain(Down, "d").fail()).rejects.toThrow("boom");

      const server = exporter
        .getFinishedSpans()
        .find((s) => s.name === "TraceDown/fail" && s.kind === SpanKind.SERVER);
      expect(server).toBeDefined();
      expect(server!.status.code).toBe(2); // SpanStatusCode.ERROR
      expect(server!.events.some((e) => e.name === "exception")).toBe(true);
    } finally {
      await silo.stop();
    }
  });
});
