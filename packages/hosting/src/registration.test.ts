import { describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { defineReducerGrain, type ReducerResult } from "@thresh/core/define-reducer-grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { createSilo } from "@thresh/hosting/silo-builder";

const Counter = defineGrain("RegistrationCounter", () => {
  let count = 0;
  return {
    increment: async (by: number) => (count += by),
    read: async () => count,
  };
});

const Echo = defineGrain("RegistrationEcho", () => ({
  echo: async (value: string) => value,
}));

/** A contract declared separately from the grain that implements it. */
interface ISeparateCounter {
  increment(by: number): Promise<number>;
}
const ISeparateCounter = defineGrainInterface<ISeparateCounter>("registration.ISeparateCounter");

const SeparatelyContracted = defineGrain("RegistrationSeparatelyContracted", () => {
  let count = 0;
  return { increment: async (by: number) => (count += by) };
});

/**
 * The `examples/thermostat` shape: one implementation answering to two
 * separately declared contracts — a device-facing one and a control-plane one.
 */
interface IThermostat {
  report(celsius: number): Promise<void>;
  reading(): Promise<number>;
}
const IThermostat = defineGrainInterface<IThermostat>("registration.IThermostat");

interface IThermostatControl {
  setTarget(celsius: number): Promise<void>;
  target(): Promise<number>;
}
const IThermostatControl = defineGrainInterface<IThermostatControl>(
  "registration.IThermostatControl",
);

const Thermostat = defineGrain("RegistrationThermostat", () => {
  let reading = 0;
  let target = 20;
  return {
    report: async (celsius: number) => {
      reading = celsius;
    },
    reading: async () => reading,
    setTarget: async (celsius: number) => {
      target = celsius;
    },
    target: async () => target,
  };
});

type CounterAction = { type: "add"; by: number };
const Tally = defineReducerGrain<{ total: number }, CounterAction>("RegistrationTally", {
  initial: () => ({ total: 0 }),
  reduce: (state, action): ReducerResult<{ total: number }> => ({
    state: { total: state.total + action.by },
  }),
});

interface IPongObserver {
  pong(value: number): Promise<void>;
}
const IPongObserver = defineGrainInterface<IPongObserver>("registration.IPongObserver");

const Notifier = defineGrain("RegistrationNotifier", () => ({
  notify: async (observer: IPongObserver, value: number) => {
    await observer.pong(value);
  },
}));

const local = new SiloAddress("silo-reg", "uid-reg", "silo-reg:19000");

function silo() {
  return createSilo({ clusterId: "registration", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork());
}

describe("SiloBuilder.registerGrain registration forms", () => {
  it("registers a fused definition and resolves it from a startup task", async () => {
    const fromStartup: number[] = [];
    const host = silo()
      .registerGrain(Counter)
      .addStartupTask(async (grains) => {
        fromStartup.push(await grains.getGrain(Counter, "boot").increment(5));
      })
      .build();

    await host.start();
    try {
      expect(fromStartup).toEqual([5]);
      expect(await host.getGrain(Counter, "boot").read()).toBe(5);
    } finally {
      await host.stop();
    }
  });

  it("keeps the embedded client's replay in lockstep: an observer created in a startup task is called back by a fused grain", async () => {
    const pongs: number[] = [];
    const host = silo()
      .registerGrain(Notifier)
      .addStartupTask(async (grains) => {
        const observer = grains.createObjectReference(IPongObserver, {
          pong: async (value: number) => {
            pongs.push(value);
          },
        });
        try {
          await grains.getGrain(Notifier, "n").notify(observer, 7);
        } finally {
          grains.deleteObjectReference(observer);
        }
      })
      .build();

    await host.start();
    try {
      expect(pongs).toEqual([7]);
    } finally {
      await host.stop();
    }
  });

  it("registers exactly the interfaces named, without also registering the fused one", async () => {
    const host = silo()
      .registerGrain(SeparatelyContracted, { interfaces: [ISeparateCounter] })
      .build();

    await host.start();
    try {
      expect(await host.getGrain(ISeparateCounter, "a").increment(2)).toBe(2);
      // The fused interface was NOT also registered: the explicit list wins.
      expect(() => host.getGrain(SeparatelyContracted, "a")).toThrow(/no grain registered/);
    } finally {
      await host.stop();
    }
  });

  it("accepts definitions and explicit ctor registrations in the same registerGrains list", async () => {
    const host = silo()
      .registerGrains([
        Counter,
        Echo,
        { ctor: SeparatelyContracted.grain, interfaces: [ISeparateCounter] },
      ])
      .build();

    await host.start();
    try {
      expect(await host.getGrain(Counter, "m").increment(3)).toBe(3);
      expect(await host.getGrain(Echo, "m").echo("hi")).toBe("hi");
      expect(await host.getGrain(ISeparateCounter, "m").increment(4)).toBe(4);
    } finally {
      await host.stop();
    }
  });

  it("serves one implementation through two separately declared interfaces", async () => {
    const host = silo()
      .registerGrain(Thermostat, { interfaces: [IThermostat, IThermostatControl] })
      .build();

    await host.start();
    try {
      await host.getGrain(IThermostatControl, "kitchen").setTarget(23);
      await host.getGrain(IThermostat, "kitchen").report(19);

      // Both interfaces address the same activation, not two of them.
      expect(await host.getGrain(IThermostat, "kitchen").reading()).toBe(19);
      expect(await host.getGrain(IThermostatControl, "kitchen").target()).toBe(23);
      // A second key is a second activation, so the state above is per-grain.
      expect(await host.getGrain(IThermostatControl, "hall").target()).toBe(20);
    } finally {
      await host.stop();
    }
  });

  it("registers only the fused interface when no list is given", async () => {
    const host = silo().registerGrain(SeparatelyContracted).build();

    await host.start();
    try {
      expect(await host.getGrain(SeparatelyContracted, "a").increment(2)).toBe(2);
      // Exactly one interface: the separately declared contract is not registered.
      expect(() => host.getGrain(ISeparateCounter, "a")).toThrow(/no grain registered/);
    } finally {
      await host.stop();
    }
  });

  it("registers a reducer grain through the bare-definition path", async () => {
    const host = silo().useMemoryStorage().registerGrain(Tally).build();

    await host.start();
    try {
      expect(await host.getGrain(Tally, "t").dispatch({ type: "add", by: 3 })).toEqual({
        total: 3,
      });
      expect(await host.getGrain(Tally, "t").query()).toEqual({ total: 3 });
    } finally {
      await host.stop();
    }
  });

  it("rejects a bare constructor with no interfaces", () => {
    expect(() => silo().registerGrain(Counter.grain, { interfaces: [] })).toThrow(/at least one/);
  });
});
