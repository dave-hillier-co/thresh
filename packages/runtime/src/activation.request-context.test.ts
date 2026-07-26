import { describe, expect, it } from "vitest";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import type { IncomingGrainCallFilter } from "@thresh/core/grain-call-filter";
import type { InvocationRequest } from "@thresh/core/request";
import { ActivationData } from "@thresh/runtime/activation";
import { requestContext } from "@thresh/runtime/invocation-context";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

class RequestContextGrain extends Grain {
  async getContextValue(key: string): Promise<string | undefined> {
    return requestContext.get(key);
  }
}

const id = new GrainId("RequestContextGrain", "a");

function makeActivation(filters: readonly IncomingGrainCallFilter[]): ActivationData {
  const activation = new ActivationData(id, new FakeTimeProvider(), 30_000, false, "act-1");
  const grain = new RequestContextGrain();
  grain.setContext(activation);
  activation.instance = grain;
  activation.incomingCallFilters = filters;
  activation.beginActivate("incoming-call");
  return activation;
}

const call = (key: string): InvocationRequest => ({
  target: id,
  interfaceId: 0,
  method: "getContextValue",
  args: [key],
  options: {},
  reentrancyId: "r1",
});

// Regression test for GAP-BUG-CALL-FILTER-REQUEST-CONTEXT: an incoming call
// filter writes to `context.headers`, but that write previously never reached
// the ambient `invocationContext` store the grain method body reads via
// `requestContext.get()` — `callMethod` built the filter context's `headers`
// as a *fresh copy* of `req.headers`, disconnected from the copy `invoke()`
// had already installed into `invocationContext`'s AsyncLocalStorage store.
describe("incoming call filter header writes reach the ambient request context", () => {
  it("makes a filter's ctx.headers write visible to requestContext.get in the method body", async () => {
    const filter: IncomingGrainCallFilter = async (ctx) => {
      ctx.headers["greeting"] = "hello";
      await ctx.invoke();
    };
    const activation = makeActivation([filter]);

    const result = await activation.invoke(call("greeting"));

    expect(result).toBe("hello");
  });

  it("still lets a filter accumulate multiple writes across a filter chain", async () => {
    const first: IncomingGrainCallFilter = async (ctx) => {
      ctx.headers["chain"] = "1";
      await ctx.invoke();
    };
    const second: IncomingGrainCallFilter = async (ctx) => {
      const existing = ctx.headers["chain"] ?? "";
      ctx.headers["chain"] = `${existing}2`;
      await ctx.invoke();
    };
    const activation = makeActivation([first, second]);

    const result = await activation.invoke(call("chain"));

    expect(result).toBe("12");
  });
});
