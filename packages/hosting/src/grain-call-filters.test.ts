import { afterEach, describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import type { IncomingGrainCallFilter } from "@tsva/core/grain-call-filter";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { createSilo } from "@tsva/hosting/silo-builder";

interface Greeter extends GrainWithStringKey {
  greet(name: string): Promise<string>;
}
const Greeter = defineGrainInterface<Greeter>("FilterGreeter");

let calls = 0;

@grain({ name: "FilterGreeter" })
class GreeterGrain extends Grain implements Greeter {
  async greet(name: string): Promise<string> {
    calls += 1;
    return `hello ${name}`;
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(...filters: IncomingGrainCallFilter[]) {
  let builder = createSilo({ clusterId: "filters", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork());
  for (const f of filters) builder = builder.addIncomingCallFilter(f);
  return builder.registerGrain(GreeterGrain, { interfaces: [Greeter] }).build();
}

describe("incoming grain call filters", () => {
  afterEach(() => {
    calls = 0;
  });

  it("wraps the method, observes context, and nests filters in registration order", async () => {
    const log: string[] = [];
    const trace: IncomingGrainCallFilter = async (ctx) => {
      log.push(`>${ctx.interfaceName}.${ctx.methodName}(${(ctx.args as string[]).join(",")})`);
      await ctx.invoke();
      log.push(`<${ctx.result as string}`);
    };
    const wrap: IncomingGrainCallFilter = async (ctx) => {
      await ctx.invoke();
      ctx.result = `[${ctx.result as string}]`;
    };

    const silo = buildSilo(trace, wrap);
    await silo.start();
    try {
      expect(await silo.getGrain(Greeter, "g").greet("x")).toBe("[hello x]");
      // trace is outermost (registered first); it sees the wrapped result.
      expect(log).toEqual([">FilterGreeter.greet(x)", "<[hello x]"]);
    } finally {
      await silo.stop();
    }
  });

  it("lets a filter rewrite arguments before the method runs", async () => {
    const upper: IncomingGrainCallFilter = async (ctx) => {
      ctx.args = [(ctx.args[0] as string).toUpperCase()];
      await ctx.invoke();
    };
    const silo = buildSilo(upper);
    await silo.start();
    try {
      expect(await silo.getGrain(Greeter, "g").greet("bob")).toBe("hello BOB");
    } finally {
      await silo.stop();
    }
  });

  it("short-circuits a call when a filter rejects without invoking", async () => {
    const auth: IncomingGrainCallFilter = async (ctx) => {
      if (ctx.args[0] === "denied") throw new Error("forbidden");
      await ctx.invoke();
    };
    const silo = buildSilo(auth);
    await silo.start();
    try {
      await expect(silo.getGrain(Greeter, "g").greet("denied")).rejects.toThrow(/forbidden/);
      expect(calls).toBe(0); // the method never ran
      expect(await silo.getGrain(Greeter, "g").greet("ok")).toBe("hello ok");
      expect(calls).toBe(1);
    } finally {
      await silo.stop();
    }
  });
});
