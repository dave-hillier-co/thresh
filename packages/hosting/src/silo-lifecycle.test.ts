import { afterEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { LogFields, Logger } from "@thresh/core/logger";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import type { TimerHandle } from "@thresh/core/time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { constructGrain } from "@thresh/runtime/construct-grain";
import { GrainService } from "@thresh/runtime/grain-service";
import { ServiceLifecycleStage as Stage } from "@thresh/runtime/lifecycle-subject";
import { createSilo, type SiloBuilder } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

/** A fake clock that can say how many timers are armed. */
class CountingClock extends FakeTimeProvider {
  private readonly armed = new Set<TimerHandle>();
  get pending(): number {
    return this.armed.size;
  }
  override setTimer(handler: () => void, delayMs: number): TimerHandle {
    const handle = super.setTimer(() => {
      this.armed.delete(handle);
      handler();
    }, delayMs);
    this.armed.add(handle);
    return handle;
  }
  override clearTimer(handle: TimerHandle): void {
    this.armed.delete(handle);
    super.clearTimer(handle);
  }
}

function recordingLogger(): {
  logger: Logger;
  errors: Array<{ message: string; fields?: LogFields }>;
} {
  const errors: Array<{ message: string; fields?: LogFields }> = [];
  const logger: Logger = {
    debug: () => undefined,
    info: () => undefined,
    warn: () => undefined,
    error: (message, fields) => {
      errors.push({ message, ...(fields !== undefined ? { fields } : {}) });
    },
  };
  return { logger, errors };
}

const hosts: SiloHost[] = [];
afterEach(async () => {
  for (const host of hosts.splice(0)) await host.stop();
});

function silo(
  network: InProcessNetwork,
  configure?: (builder: SiloBuilder) => void,
  config: Partial<Parameters<typeof createSilo>[0]> = {},
): SiloHost {
  const builder = createSilo({
    clusterId: "lifecycle",
    local,
    selfProbe: { enabled: false },
    ...config,
  })
    .useStaticMembership([local])
    .useInProcessTransport(network)
    .disableTransactions();
  configure?.(builder);
  const host = builder.build();
  hosts.push(host);
  return host;
}

describe("the silo lifecycle", () => {
  it("stops participants in exactly the reverse of the order it started them", async () => {
    const log: string[] = [];
    const host = silo(new InProcessNetwork(), (builder) => {
      for (const [name, stage] of [
        ["storage", Stage.RuntimeStorageServices],
        ["initialize", Stage.RuntimeInitialize],
        ["application", Stage.ApplicationServices],
        ["services", Stage.RuntimeServices],
        ["grain-services", Stage.RuntimeGrainServices],
        ["active", Stage.Active],
      ] as const) {
        builder.addLifecycleParticipant(name, stage, {
          start: async () => void log.push(`start:${name}`),
          stop: async () => void log.push(`stop:${name}`),
        });
      }
    });
    await host.start();
    await host.stop();
    const order = ["initialize", "services", "storage", "grain-services", "application", "active"];
    expect(log).toEqual([
      ...order.map((n) => `start:${n}`),
      ...[...order].reverse().map((n) => `stop:${n}`),
    ]);
  });

  it("logs a participant whose stop throws and still runs the stops after it", async () => {
    const log: string[] = [];
    const { logger, errors } = recordingLogger();
    const host = silo(
      new InProcessNetwork(),
      (builder) => {
        builder.addLifecycleParticipant("late", Stage.RuntimeGrainServices, {
          stop: async () => void log.push("stop:late"),
        });
        builder.addLifecycleParticipant("broken", Stage.RuntimeStorageServices, {
          stop: async () => {
            throw new Error("broken stop");
          },
        });
        builder.addLifecycleParticipant("early", Stage.RuntimeInitialize, {
          stop: async () => void log.push("stop:early"),
        });
      },
      { logger },
    );
    await host.start();
    await expect(host.stop()).resolves.toBeUndefined();
    expect(log).toEqual(["stop:late", "stop:early"]);
    const failure = errors.find((e) => e.message === "silo stop hook failed");
    expect(failure?.fields).toMatchObject({ phase: "broken" });
    expect(String((failure?.fields as { error?: unknown }).error)).toContain("broken stop");
  });

  it("rolls back every stage that started when a start fails part-way, and rejects with the original error", async () => {
    const network = new InProcessNetwork();
    const clock = new CountingClock();
    const log: string[] = [];
    const host = silo(
      network,
      (builder) => {
        builder.addLifecycleParticipant("client", Stage.RuntimeInitialize, {
          start: async () => void log.push("start:client"),
          stop: async () => void log.push("stop:client"),
        });
        builder.addLifecycleParticipant("never-reached", Stage.Last, {
          start: async () => {
            throw new Error("the original failure");
          },
        });
      },
      { time: clock, selfProbe: { enabled: true } },
    );

    await expect(host.start()).rejects.toThrow("the original failure");

    expect(network.lookup(local)).toBeUndefined();
    expect(clock.pending).toBe(0);
    expect(log).toEqual(["start:client", "stop:client"]);
    expect(host.health.ready().ok).toBe(false);
  });

  it("arms the self-probe timer and binds the listener on a healthy start (control for the rollback test)", async () => {
    const network = new InProcessNetwork();
    const clock = new CountingClock();
    const host = silo(network, undefined, { time: clock, selfProbe: { enabled: true } });
    await host.start();
    expect(network.lookup(local)).toBeDefined();
    expect(clock.pending).toBeGreaterThan(0);
    await host.stop();
    expect(network.lookup(local)).toBeUndefined();
    expect(clock.pending).toBe(0);
  });

  it("stops a grain service before the client registered ahead of it is closed", async () => {
    const log: string[] = [];
    class Service extends GrainService {
      override stop(): void {
        log.push("stop:grain-service");
        super.stop();
      }
    }
    const host = silo(new InProcessNetwork(), (builder) => {
      // The storage-client slot: closed on the way out of RuntimeInitialize.
      builder.addLifecycleParticipant("storage-client", Stage.RuntimeInitialize, {
        stop: async () => void log.push("close:storage-client"),
      });
      builder.addGrainService("svc", () => new Service());
    });
    await host.start();
    await host.stop();
    expect(log).toEqual(["stop:grain-service", "close:storage-client"]);
  });

  it("flips readiness before it stops pre-deactivation participants, and those before the node deactivates", async () => {
    const log: string[] = [];
    const seen: { health?: SiloHost["health"] } = {};
    const host = silo(new InProcessNetwork(), (builder) => {
      builder.addLifecycleParticipant("agents", Stage.BecomeActive, {
        stop: async () => void log.push(`stop:agents ready=${seen.health?.ready().ok}`),
      });
      builder.addLifecycleParticipant("after-node", Stage.RuntimeInitialize, {
        stop: async () => void log.push("stop:after-node"),
      });
    });
    seen.health = host.health;
    await host.start();
    await host.stop();
    expect(log).toEqual(["stop:agents ready=false", "stop:after-node"]);
  });
});

describe("grain activators compose", () => {
  interface IAlpha extends GrainKey<string> {
    who(): Promise<string>;
  }
  interface IBeta extends GrainKey<string> {
    who(): Promise<string>;
  }
  const IAlpha = defineGrainInterface<IAlpha>("IAlpha.lifecycle");
  const IBeta = defineGrainInterface<IBeta>("IBeta.lifecycle");

  @grain()
  class AlphaGrain extends Grain implements IAlpha {
    constructor(private readonly label = "default") {
      super();
    }
    async who(): Promise<string> {
      return `alpha:${this.label}`;
    }
  }
  @grain()
  class BetaGrain extends Grain implements IBeta {
    constructor(private readonly label = "default") {
      super();
    }
    async who(): Promise<string> {
      return `beta:${this.label}`;
    }
  }

  it("applies each activator to the classes it claims", async () => {
    const host = silo(new InProcessNetwork(), (builder) => {
      builder
        .registerGrain(AlphaGrain, { interfaces: [IAlpha] })
        .registerGrain(BetaGrain, { interfaces: [IBeta] })
        .useGrainActivator({
          claims: (ctor) => ctor === AlphaGrain,
          createInstance: () => new AlphaGrain("from-a"),
        })
        .useGrainActivator({
          claims: (ctor) => ctor === BetaGrain,
          createInstance: () => new BetaGrain("from-b"),
        });
    });
    await host.start();
    expect(await host.getGrain(IAlpha, "x").who()).toBe("alpha:from-a");
    expect(await host.getGrain(IBeta, "x").who()).toBe("beta:from-b");
  });

  it("still lets one activator claim every class, falling through to construction", async () => {
    const host = silo(new InProcessNetwork(), (builder) => {
      builder
        .registerGrain(AlphaGrain, { interfaces: [IAlpha] })
        .registerGrain(BetaGrain, { interfaces: [IBeta] })
        .useGrainActivator({
          createInstance: (ctor) =>
            ctor === AlphaGrain ? new AlphaGrain("switch") : constructGrain(ctor),
        });
    });
    await host.start();
    expect(await host.getGrain(IAlpha, "x").who()).toBe("alpha:switch");
    expect(await host.getGrain(IBeta, "x").who()).toBe("beta:default");
  });

  it("routes disposal to the activator that created the instance", async () => {
    const disposed: string[] = [];
    const host = silo(new InProcessNetwork(), (builder) => {
      builder
        .registerGrain(AlphaGrain, { interfaces: [IAlpha] })
        .registerGrain(BetaGrain, { interfaces: [IBeta] })
        .useGrainActivator({
          claims: (ctor) => ctor === AlphaGrain,
          createInstance: () => new AlphaGrain(),
          disposeInstance: () => void disposed.push("a"),
        })
        .useGrainActivator({
          claims: (ctor) => ctor === BetaGrain,
          createInstance: () => new BetaGrain(),
          disposeInstance: () => void disposed.push("b"),
        });
    });
    await host.start();
    await host.getGrain(IBeta, "x").who();
    await host.stop();
    expect(disposed).toEqual(["b"]);
  });

  it("refuses an activator registered behind one that already claims every class", () => {
    const builder = createSilo({ clusterId: "c", local });
    builder.useGrainActivator({ createInstance: (ctor) => constructGrain(ctor) });
    expect(() =>
      builder.useGrainActivator({
        claims: () => true,
        createInstance: (ctor) => constructGrain(ctor),
      }),
    ).toThrow(/claims every grain class/);
  });
});
