import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { runCallFilters, type GrainCallContext } from "@thresh/core/grain-call-filter";

type GrainCallFilter = (context: GrainCallContext) => Promise<void>;

function context(args: unknown[]): GrainCallContext {
  return {
    target: new GrainId("T" as GrainType, "k"),
    source: undefined,
    interfaceId: 1,
    interfaceName: "T",
    methodName: "m",
    args,
    result: undefined,
    headers: {},
    invoke: async () => {},
  };
}

describe("call-filter pipeline", () => {
  it("runs filters around the terminal in order, then returns the result", async () => {
    const order: string[] = [];
    const a: GrainCallFilter = async (ctx) => {
      order.push("a:before");
      await ctx.invoke();
      order.push("a:after");
    };
    const b: GrainCallFilter = async (ctx) => {
      order.push("b:before");
      await ctx.invoke();
      order.push("b:after");
    };
    const ctx = context([]);
    const result = await runCallFilters([a, b], ctx, async () => {
      order.push("method");
      return 42;
    });
    expect(result).toBe(42);
    expect(order).toEqual(["a:before", "b:before", "method", "b:after", "a:after"]);
  });

  it("lets a filter rewrite arguments before the method sees them", async () => {
    const doubling: GrainCallFilter = async (ctx) => {
      ctx.args = (ctx.args as number[]).map((n) => n * 2);
      await ctx.invoke();
    };
    const ctx = context([3, 4]);
    const result = await runCallFilters([doubling], ctx, async () => ctx.args);
    expect(result).toEqual([6, 8]);
  });

  it("short-circuits when a filter sets a result without calling invoke()", async () => {
    let methodRan = false;
    const cache: GrainCallFilter = async (ctx) => {
      ctx.result = "cached"; // serve a value without proceeding
    };
    const ctx = context([]);
    const result = await runCallFilters([cache], ctx, async () => {
      methodRan = true;
      return "fresh";
    });
    expect(result).toBe("cached");
    expect(methodRan).toBe(false);
  });

  it("short-circuits cleanly when a filter sets result to undefined without calling invoke()", async () => {
    // A void-returning method (or a legitimately cached `undefined`) is a valid
    // short-circuit value — it must not be mistaken for "no result was set".
    let methodRan = false;
    const voidCache: GrainCallFilter = async (ctx) => {
      ctx.result = undefined; // intentional short-circuit, not a broken chain
    };
    const ctx = context([]);
    const result = await runCallFilters([voidCache], ctx, async () => {
      methodRan = true;
      return "fresh";
    });
    expect(result).toBeUndefined();
    expect(methodRan).toBe(false);
  });

  it("rejects when a filter neither calls invoke() nor sets a result", async () => {
    let methodRan = false;
    const broken: GrainCallFilter = async () => {
      // returns without proceeding and without producing a result — an error
    };
    const ctx = context([]);
    await expect(
      runCallFilters([broken], ctx, async () => {
        methodRan = true;
        return "fresh";
      }),
    ).rejects.toThrow(/did not call context\.invoke\(\)/);
    expect(methodRan).toBe(false);
  });

  it("lets a filter observe and replace the result", async () => {
    const wrap: GrainCallFilter = async (ctx) => {
      await ctx.invoke();
      ctx.result = `[${ctx.result as string}]`;
    };
    const ctx = context([]);
    const result = await runCallFilters([wrap], ctx, async () => "x");
    expect(result).toBe("[x]");
  });

  it("propagates an error thrown by the method to the surrounding filter", async () => {
    let caught: unknown;
    const guard: GrainCallFilter = async (ctx) => {
      try {
        await ctx.invoke();
      } catch (err) {
        caught = err;
        ctx.result = "recovered";
      }
    };
    const ctx = context([]);
    const result = await runCallFilters([guard], ctx, async () => {
      throw new Error("boom");
    });
    expect((caught as Error).message).toBe("boom");
    expect(result).toBe("recovered");
  });
});
