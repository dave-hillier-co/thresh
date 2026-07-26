import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

interface ICounter extends GrainWithStringKey {
  increment(by: number): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("ICounter.hosting");

@grain()
class CounterGrain extends Grain implements ICounter {
  private count = 0;
  async increment(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

interface IBlocker extends GrainWithStringKey {
  block(): Promise<string>;
}
const IBlocker = defineGrainInterface<IBlocker>("IBlocker.hosting");

/** Module-scoped so the test can hold a call open until it decides to release it. */
let blockerGate: { resolve: () => void; promise: Promise<void> };
function resetBlockerGate(): void {
  let resolve!: () => void;
  const promise = new Promise<void>((res) => {
    resolve = res;
  });
  blockerGate = { resolve, promise };
}

@grain()
class BlockerGrain extends Grain implements IBlocker {
  async block(): Promise<string> {
    await blockerGate.promise;
    return "done";
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildHost() {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMessagePackSerialization()
    .registerGrain(CounterGrain, { interfaces: [ICounter] })
    .build();
}

describe("createSilo / SiloHost", () => {
  it("is not ready until started, ready after, and serves grain calls", async () => {
    const host = buildHost();
    expect(host.health.ready().ok).toBe(false);

    await host.start();
    try {
      expect(host.health.ready().ok).toBe(true);
      expect(await host.getGrain(ICounter, "x").increment(5)).toBe(5);
      expect(host.isActive(new GrainId("Counter", "x"))).toBe(true);
    } finally {
      await host.stop();
    }
  });

  it("flips readiness off when drained", async () => {
    const host = buildHost();
    await host.start();
    expect(host.health.ready().ok).toBe(true);
    await host.stop();
    expect(host.health.ready().ok).toBe(false);
  });

  it("refuses to build without membership or transport", () => {
    expect(() => createSilo({ clusterId: "c1", local }).build()).toThrow(/membership/);
    expect(() =>
      createSilo({ clusterId: "c1", local }).useStaticMembership([local]).build(),
    ).toThrow(/transport/);
  });
});

describe("createSilo scheduler back-pressure (Orleans SchedulingOptions parity)", () => {
  it("rejects a call once a configured maxEnqueuedRequestsHardLimit is exceeded", async () => {
    resetBlockerGate();
    const host = createSilo({
      clusterId: "c1",
      local,
      scheduling: { maxEnqueuedRequestsHardLimit: 1 },
    })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMessagePackSerialization()
      .registerGrain(BlockerGrain, { interfaces: [IBlocker] })
      .build();
    await host.start();
    try {
      const blocker = host.getGrain(IBlocker, "x");
      // Fire several concurrent calls at the same activation: with the queue
      // capped at 1, at least one must be rejected as over the hard limit
      // rather than growing the queue without bound; whichever are admitted
      // still complete once the gate opens.
      const calls = Array.from({ length: 5 }, () => blocker.block());
      blockerGate.resolve();
      const results = await Promise.allSettled(calls);
      expect(results.some((r) => r.status === "rejected")).toBe(true);
      expect(results.some((r) => r.status === "fulfilled" && r.value === "done")).toBe(true);
    } finally {
      await host.stop();
    }
  });
});
