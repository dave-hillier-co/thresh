import { afterEach, describe, expect, it, vi } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { LogFields, Logger } from "@thresh/core/logger";
import type { ReminderEntry } from "@thresh/core/reminder";
import { SiloAddress } from "@thresh/core/silo-address";
import { KubernetesEndpointWatch } from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import type { EndpointSliceSource } from "@thresh/clustering-k8s/kubernetes-endpoint-watch";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryReminderTable } from "@thresh/reminders/memory-reminder-table";
import { Silo } from "@thresh/runtime/silo";
import { createSilo } from "@thresh/hosting/silo-builder";
import { waitFor } from "@thresh/testing/wait";

/**
 * The silo's logger is one seam: `SiloConfig.logger` reaches the node, the dispatchers, the
 * activations, the reminder service, the Kubernetes endpoint watch and the self-probe, WITHOUT
 * `useLogging`'s line per call. With no logger at all, warnings reach the console.
 */

interface Entry {
  level: string;
  message: string;
  fields?: LogFields;
}

function recorder(): { logger: Logger; entries: Entry[] } {
  const entries: Entry[] = [];
  const at =
    (level: string) =>
    (message: string, fields?: LogFields): void => {
      entries.push({ level, message, ...(fields !== undefined ? { fields } : {}) });
    };
  return {
    entries,
    logger: { debug: at("debug"), info: at("info"), warn: at("warn"), error: at("error") },
  };
}

interface Probe extends GrainKey<string> {
  ping(): Promise<string>;
  boom(): Promise<void>;
}
const Probe = defineGrainInterface<Probe>("SiloLoggerProbe", {
  options: { boom: { oneWay: true } },
});

@grain({ name: "SiloLoggerProbe", placement: "preferLocal" })
class ProbeGrain extends Grain implements Probe {
  async ping(): Promise<string> {
    return "pong";
  }
  async boom(): Promise<void> {
    throw new Error("one-way boom");
  }
  override async onDeactivate(): Promise<void> {
    throw new Error("deactivate boom");
  }
}

/** The Kubernetes API is down: every list fails. The true boundary, faked. */
const downSource: EndpointSliceSource = {
  list: () => Promise.reject(new Error("api server down")),
  watch: () => () => undefined,
};

const a = new SiloAddress("silo-a", "uid-a", "silo-a:11111");
const b = new SiloAddress("silo-b", "uid-b", "silo-b:11111");

afterEach(() => vi.restoreAllMocks());

describe("SiloConfig.logger", () => {
  it("reaches the dispatcher and the activations, and does not log each call", async () => {
    const { logger, entries } = recorder();
    const host = createSilo({
      clusterId: "l1",
      local: a,
      logger,
      selfProbe: { enabled: false },
      gracefulShutdownMs: 0,
    })
      .useStaticMembership([a])
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(ProbeGrain, { interfaces: [Probe] })
      .build();
    await host.start();
    try {
      const grain = host.getGrain(Probe, "k");
      await grain.ping();
      await grain.boom(); // detached one-way failure: the dispatcher logs it
      await waitFor(() => entries.some((e) => e.message === "one-way grain call failed"));
    } finally {
      await host.stop();
    }
    // the activation's own failure (onDeactivate threw during stop)
    expect(entries.some((e) => e.message === "onDeactivate hook failed")).toBe(true);
    expect(entries.some((e) => e.message === "grain call")).toBe(false);
    expect(entries.filter((e) => e.level === "info" || e.level === "debug")).toEqual([]);
  });

  it("reaches the reminder service", async () => {
    const { logger, entries } = recorder();
    class FailingTable extends MemoryReminderTable {
      override async readRange(): Promise<ReminderEntry[]> {
        throw new Error("table down");
      }
    }
    const host = createSilo({
      clusterId: "l2",
      local: a,
      logger,
      selfProbe: { enabled: false },
      gracefulShutdownMs: 0,
    })
      .useStaticMembership([a])
      .useInProcessTransport(new InProcessNetwork())
      .useReminders(new FailingTable())
      .registerGrain(ProbeGrain, { interfaces: [Probe] })
      .build();
    await host.start();
    await host.stop();
    expect(entries.some((e) => e.message === "reminder reconcile failed")).toBe(true);
  });

  it("reaches the Kubernetes endpoint watch", async () => {
    const { logger, entries } = recorder();
    const watch = new KubernetesEndpointWatch(downSource, { reconnectMs: 60_000 });
    const host = createSilo({
      clusterId: "l3",
      local: a,
      logger,
      selfProbe: { enabled: false },
      gracefulShutdownMs: 0,
    })
      .useKubernetesMembership(watch, { portName: "silo" })
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(ProbeGrain, { interfaces: [Probe] })
      .build();
    try {
      await host.start();
      expect(entries.some((e) => e.message === "kubernetes endpoint relist failed")).toBe(true);
    } finally {
      await host.stop();
    }
  });

  it("reaches the self-probe", async () => {
    const { logger, entries } = recorder();
    const host = createSilo({
      clusterId: "l4",
      local: a,
      logger,
      selfProbe: { intervalMs: 10, timeoutMs: 10, missedThreshold: 1 },
    })
      .useStaticMembership([a])
      .useInProcessTransport(new InProcessNetwork())
      // A filter that fails every call made to the self-probe grain: the dispatcher "hangs".
      .addIncomingCallFilter(async (ctx) => {
        if (ctx.interfaceName === "Thresh.Runtime.ISiloProbe") throw new Error("wedged");
        await ctx.invoke();
      })
      .registerGrain(ProbeGrain, { interfaces: [Probe] })
      .build();
    await host.start();
    try {
      await waitFor(() => entries.some((e) => e.message.startsWith("self-probe failed")));
      expect(host.health.ready().ok).toBe(false);
    } finally {
      await host.stop();
    }
  });

  it("logs an inbound one-way request that fails over the wire, rather than discarding it", async () => {
    const network = new InProcessNetwork();
    const callee = recorder();
    const caller = recorder();
    const build = (local: SiloAddress, logger: Logger) =>
      createSilo({
        clusterId: "l5",
        local,
        logger,
        selfProbe: { enabled: false },
        gracefulShutdownMs: 0,
      })
        .useStaticMembership([a, b], undefined, { allowWithoutLiveness: true })
        .useInProcessTransport(network)
        .registerGrain(ProbeGrain, { interfaces: [Probe] })
        .build();
    const hostA = build(a, callee.logger);
    const hostB = build(b, caller.logger);
    await hostA.start();
    await hostB.start();
    try {
      // preferLocal: the grain lives on A, so B's call crosses the wire.
      await hostA.getGrain(Probe, "k").ping();
      await hostB.getGrain(Probe, "k").boom();
      await waitFor(() => callee.entries.some((e) => e.message === "one-way grain call failed"));
      expect(caller.entries.some((e) => e.message === "one-way grain call failed")).toBe(false);
    } finally {
      await hostB.stop();
      await hostA.stop();
    }
  });
});

describe("a silo with no logger", () => {
  it("writes a warning to the console when the endpoint watch fails, and nothing at debug or info", async () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => undefined);
    const quiet = (["log", "info", "debug"] as const).map((m) =>
      vi.spyOn(console, m).mockImplementation(() => undefined),
    );
    const host = createSilo({
      clusterId: "l6",
      local: a,
      selfProbe: { enabled: false },
      gracefulShutdownMs: 0,
    })
      .useKubernetesMembership(new KubernetesEndpointWatch(downSource, { reconnectMs: 60_000 }), {
        portName: "silo",
      })
      .useInProcessTransport(new InProcessNetwork())
      .registerGrain(ProbeGrain, { interfaces: [Probe] })
      .build();
    try {
      await host.start();
    } finally {
      await host.stop();
    }
    expect(warn.mock.calls.map((c) => String(c[0]))).toContain(
      "[thresh] kubernetes endpoint relist failed",
    );
    expect(quiet.flatMap((s) => s.mock.calls)).toEqual([]);
  });
});

describe("a Silo-hosted (non-createSilo) host", () => {
  it("logs a detached one-way failure through the logger it was given", async () => {
    const { logger, entries } = recorder();
    const silo = new Silo({ logger });
    silo.registerGrain(ProbeGrain, { interfaces: [Probe] });
    silo.start();
    try {
      await silo.getGrain(Probe, "k").boom();
      await waitFor(() => entries.some((e) => e.message === "one-way grain call failed"));
    } finally {
      await silo.stop();
    }
  });
});
