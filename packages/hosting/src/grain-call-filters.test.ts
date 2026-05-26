import { afterEach, describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { defineGrain } from "@tsva/core/define-grain";
import { Grain } from "@tsva/core/grain";
import {
  INCOMING_CALL_FILTER,
  type IncomingGrainCallContext,
  type IncomingGrainCallFilter,
  type OutgoingGrainCallFilter,
} from "@tsva/core/grain-call-filter";
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

// A grain that filters its own incoming calls (Orleans IIncomingGrainCallFilter).
interface Echoer extends GrainWithStringKey {
  echo(s: string): Promise<string>;
}
const Echoer = defineGrainInterface<Echoer>("FilterEchoer");
const FnEchoer = defineGrainInterface<Echoer>("FilterFnEchoer");

let order: string[] = [];

@grain({ name: "FilterEchoer" })
class EchoerGrain extends Grain implements Echoer {
  async echo(s: string): Promise<string> {
    order.push("method");
    return s;
  }
  async [INCOMING_CALL_FILTER](ctx: IncomingGrainCallContext): Promise<void> {
    order.push("grain:before");
    await ctx.invoke();
    order.push("grain:after");
    ctx.result = `[${ctx.result as string}]`;
  }
}

// Functional counterpart declaring the self-filter under the same symbol.
const FnEchoerGrain = defineGrain<Echoer>("FilterFnEchoer", () => ({
  echo: async (s: string) => s,
  [INCOMING_CALL_FILTER]: async (ctx: IncomingGrainCallContext) => {
    await ctx.invoke();
    ctx.result = `fn(${ctx.result as string})`;
  },
}));

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(...filters: IncomingGrainCallFilter[]) {
  let builder = createSilo({ clusterId: "filters", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork());
  for (const f of filters) builder = builder.addIncomingCallFilter(f);
  return builder.registerGrain(GreeterGrain, { interfaces: [Greeter] }).build();
}

function buildSiloOut(...filters: OutgoingGrainCallFilter[]) {
  let builder = createSilo({ clusterId: "filters-out", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork());
  for (const f of filters) builder = builder.addOutgoingCallFilter(f);
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

describe("outgoing grain call filters", () => {
  afterEach(() => {
    calls = 0;
  });

  it("wraps an outbound call at the caller, observing target and method", async () => {
    const seen: string[] = [];
    const trace: OutgoingGrainCallFilter = async (ctx) => {
      seen.push(`${ctx.interfaceName}.${ctx.methodName}->${ctx.target.toString()}`);
      await ctx.invoke();
      ctx.result = `(${ctx.result as string})`;
    };
    const silo = buildSiloOut(trace);
    await silo.start();
    try {
      expect(await silo.getGrain(Greeter, "g").greet("x")).toBe("(hello x)");
      expect(seen).toEqual(["FilterGreeter.greet->FilterGreeter/g"]);
    } finally {
      await silo.stop();
    }
  });

  it("short-circuits an outbound call without dispatching (client-side cache)", async () => {
    const cache: OutgoingGrainCallFilter = async (ctx) => {
      ctx.result = "cached"; // serve without reaching the grain
    };
    const silo = buildSiloOut(cache);
    await silo.start();
    try {
      expect(await silo.getGrain(Greeter, "g").greet("x")).toBe("cached");
      expect(calls).toBe(0); // the call never left the caller
    } finally {
      await silo.stop();
    }
  });
});

describe("per-grain incoming filters", () => {
  afterEach(() => {
    order = [];
  });

  it("runs a grain's own filter around its methods", async () => {
    const silo = createSilo({ clusterId: "self-filter", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(EchoerGrain, { interfaces: [Echoer] })
      .build();
    await silo.start();
    try {
      expect(await silo.getGrain(Echoer, "e").echo("x")).toBe("[x]");
      expect(order).toEqual(["grain:before", "method", "grain:after"]);
    } finally {
      await silo.stop();
    }
  });

  it("nests the grain's own filter inside the silo-wide filters", async () => {
    const outer: IncomingGrainCallFilter = async (ctx) => {
      order.push("silo:before");
      await ctx.invoke();
      order.push("silo:after");
    };
    const silo = createSilo({ clusterId: "self-filter-2", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .addIncomingCallFilter(outer)
      .registerGrain(EchoerGrain, { interfaces: [Echoer] })
      .build();
    await silo.start();
    try {
      await silo.getGrain(Echoer, "e").echo("x");
      // silo filter outermost, grain's own filter innermost, then the method.
      expect(order).toEqual(["silo:before", "grain:before", "method", "grain:after", "silo:after"]);
    } finally {
      await silo.stop();
    }
  });

  it("supports a self-filter declared by a functional grain", async () => {
    const silo = createSilo({ clusterId: "self-filter-fn", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(FnEchoerGrain, { interfaces: [FnEchoer] })
      .build();
    await silo.start();
    try {
      expect(await silo.getGrain(FnEchoer, "e").echo("y")).toBe("fn(y)");
    } finally {
      await silo.stop();
    }
  });
});
