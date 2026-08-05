// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainCallTraceContextPropagationTests.cs
// @ v10.1.0 (MIT). Upstream declares `ITraceContextPropagationGrain` and its
// `TraceContextInfo` DTO inline in the test file itself.
//
// Upstream's `TraceContextInfo` also carries `ActivityId`, `OperationName`,
// `Kind`, and `IsRemote`, read off `System.Diagnostics.Activity.Current`
// inside the grain. This port's grain only reports what a grain can observe
// about its own current OpenTelemetry span through the public `@opentelemetry/api`
// surface (trace/span id, whether one is active, and the raw `traceparent`
// header) — `Kind`/`IsRemote`/parent linkage are instead asserted from the
// captured finished spans in `@thresh/parity/support/tracing`'s in-memory
// exporter, which is the SAME data upstream's `Started` `ConcurrentBag`
// (populated by an `ActivityListener`) captures for its own linking
// assertions (`ClientAndServerSpansAreProperlyLinked`, etc).
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";

export interface TraceContextInfo {
  readonly hasActivity: boolean;
  readonly traceId: string | undefined;
  readonly spanId: string | undefined;
  readonly traceParentFromRequestContext: string | undefined;
}

export interface ITraceContextPropagationGrain extends GrainKey<bigint> {
  /** Reports the server-side trace context visible from inside this activation. */
  getTraceContextInfo(): Promise<TraceContextInfo>;
  /**
   * Reports this activation's own trace context, then calls the next grain
   * (`key + 1`) and reports its trace context too — used to verify
   * propagation across a grain-to-grain call.
   */
  getNestedTraceContextInfo(): Promise<{ local: TraceContextInfo; nested: TraceContextInfo }>;
}

export const ITraceContextPropagationGrain = defineGrainInterface<ITraceContextPropagationGrain>(
  "UnitTests.GrainInterfaces.ITraceContextPropagationGrain",
);
