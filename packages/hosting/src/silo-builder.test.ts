import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { constructGrain } from "@thresh/runtime/construct-grain";
import { createSilo, type Registration } from "@thresh/hosting/silo-builder";

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

// ---------------------------------------------------------------------------
// Registration typing (issue #58)
// ---------------------------------------------------------------------------

interface IGreeter extends GrainWithStringKey {
  greet(): Promise<string>;
}
const IGreeter = defineGrainInterface<IGreeter>("IGreeter.hosting");

/**
 * The normal shape of a grain once there is no DI container: the constructor
 * takes an options bag, and the silo's `GrainActivator` is the seam that hands
 * it one. Registering this must type-check without a cast.
 */
@grain()
class GreeterGrain extends Grain implements IGreeter {
  constructor(private readonly options: { greeting: string }) {
    super();
  }

  async greet(): Promise<string> {
    return `${this.options.greeting} from ${this.id.key}`;
  }
}

/**
 * A consumer's own shared registration list, named by the exported type rather
 * than restated structurally, and `readonly` so it can be a module constant
 * without callers having to copy it.
 */
const HOSTED_GRAINS: readonly Registration[] = [
  { ctor: GreeterGrain, interfaces: [IGreeter] },
  { ctor: CounterGrain, interfaces: [ICounter] },
];

describe("SiloBuilder grain registration typing", () => {
  it("registers a grain whose constructor takes an options bag, built by the activator", async () => {
    const built: string[] = [];
    const host = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .useMessagePackSerialization()
      .registerGrains(HOSTED_GRAINS)
      .useGrainActivator({
        createInstance: (ctor) => {
          built.push(ctor.name);
          // No cast at the call site: the activator is handed the registered
          // class, and constructs the ones it knows about itself.
          if (ctor === GreeterGrain) return new GreeterGrain({ greeting: "hello" });
          return constructGrain(ctor);
        },
      })
      .build();

    await host.start();
    try {
      expect(await host.getGrain(IGreeter, "x").greet()).toBe("hello from x");
      // Every other grain type on the silo still falls through to `new ctor()`.
      expect(await host.getGrain(ICounter, "y").increment(3)).toBe(3);
    } finally {
      await host.stop();
    }
    expect(built).toContain("GreeterGrain");
    expect(built).toContain("CounterGrain");
  });
});

// ---------------------------------------------------------------------------
// The observer seam and the transport that can back it (issue #55)
// ---------------------------------------------------------------------------

interface IObserverSeam {
  notify(value: string): Promise<void>;
}
const IObserverSeam = defineGrainInterface<IObserverSeam>("IObserverSeam.hosting");

describe("SiloBuilder observer hosting (createObjectReference from a startup task)", () => {
  it("hosts an observer from a startup task when the seam is declared and backed", async () => {
    let reference: IObserverSeam | undefined;
    const host = createSilo({ clusterId: "c1", local })
      .useStaticMembership([local])
      .useInProcessTransport(new InProcessNetwork())
      .requireObserverHosting()
      .registerGrain(CounterGrain, { interfaces: [ICounter] })
      .addStartupTask(async (grains) => {
        reference = grains.createObjectReference(IObserverSeam, {
          notify: async () => undefined,
        });
        grains.deleteObjectReference(reference);
      })
      .build();

    await host.start();
    try {
      expect(reference).toBeDefined();
    } finally {
      await host.stop();
    }
  });

  it("refuses to build when the declared observer seam has no transport that can back it", () => {
    expect(() =>
      createSilo({ clusterId: "c1", local })
        .useStaticMembership([local])
        .useWebSocketTransport()
        .requireObserverHosting()
        .addStartupTask(async (grains) => {
          grains.createObjectReference(IObserverSeam, { notify: async () => undefined });
        })
        .build(),
    ).toThrow(/requireObserverHosting.*useInProcessTransport/s);
  });

  it("still builds a WebSocket silo whose startup tasks never host an observer", () => {
    // The gate is the explicit declaration, not the mere presence of a startup
    // task: a silo that registers startup tasks for any other reason (SpaceDB's
    // `addSpiceportGrainServices` registers one unconditionally) must keep
    // building on a WebSocket transport, as the examples in this repo do.
    expect(() =>
      createSilo({ clusterId: "c1", local })
        .useStaticMembership([local])
        .useWebSocketTransport()
        .registerGrain(CounterGrain, { interfaces: [ICounter] })
        .addStartupTask(async (grains) => {
          await grains.getGrain(ICounter, "x").increment(1);
        })
        .build(),
    ).not.toThrow();
  });
});
