import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo, type SiloConfig } from "@thresh/hosting/silo-builder";

/** A silo's `localCallSemantics`, and what a production silo does when it sets none. */
const received: unknown[] = [];

const Box = defineGrain("LocalCallSemanticsBox", () => ({
  put: async (value: { n: number }): Promise<void> => {
    received.push(value);
  },
}));

const stops: Array<() => Promise<void>> = [];
afterEach(async () => {
  for (const stop of stops.splice(0)) await stop();
  received.length = 0;
});

async function silo(config: Partial<SiloConfig> = {}) {
  const local = new SiloAddress("solo", "uid-solo", "solo:11111");
  const host = createSilo({ clusterId: "local-call-semantics", local, ...config })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .registerGrain(Box)
    .build();
  await host.start();
  stops.push(() => host.stop());
  return host;
}

describe("SiloConfig.localCallSemantics", () => {
  it('defaults to "reference": a same-silo callee receives the caller\'s own object', async () => {
    const host = await silo();
    const value = { n: 1 };
    await host.getGrain(Box, "b").put(value);
    expect(received[0]).toBe(value);
  });

  it.each(["copy", "wire"] as const)('copies the argument under "%s"', async (semantics) => {
    const host = await silo({ localCallSemantics: semantics });
    const value = { n: 1 };
    await host.getGrain(Box, "b").put(value);
    expect(received[0]).not.toBe(value);
    expect(received[0]).toEqual(value);
  });
});
