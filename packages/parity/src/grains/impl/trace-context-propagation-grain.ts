// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainCallTraceContextPropagationTests.cs
// @ v10.1.0 (MIT) — `TraceContextPropagationGrain`.
import { trace } from "@opentelemetry/api";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { requestContext } from "@thresh/runtime/invocation-context";
import {
  ITraceContextPropagationGrain,
  type TraceContextInfo,
} from "@thresh/parity/grains/interfaces/trace-context-propagation-grain-interfaces";

export { ITraceContextPropagationGrain };

@grain({ name: "UnitTests.Grains.TraceContextPropagationGrain", placement: "random" })
export class TraceContextPropagationGrain extends Grain implements ITraceContextPropagationGrain {
  async getTraceContextInfo(): Promise<TraceContextInfo> {
    const span = trace.getActiveSpan();
    const spanContext = span?.spanContext();
    return {
      hasActivity: span !== undefined,
      traceId: spanContext?.traceId,
      spanId: spanContext?.spanId,
      traceParentFromRequestContext: requestContext.get("traceparent"),
    };
  }

  async getNestedTraceContextInfo(): Promise<{
    local: TraceContextInfo;
    nested: TraceContextInfo;
  }> {
    const local = await this.getTraceContextInfo();
    const nestedKey = (this.id.key as bigint) + 1n;
    const nested = await this.getGrain(
      ITraceContextPropagationGrain,
      nestedKey,
    ).getTraceContextInfo();
    return { local, nested };
  }
}
