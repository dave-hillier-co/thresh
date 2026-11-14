import { describe, expect, it } from "vitest";
import type { LogFields, Logger } from "@thresh/core/logger";
import { GracefulShutdown } from "@thresh/hosting/graceful-shutdown";
import { HealthCheck } from "@thresh/hosting/health-check";
import { SiloHost, type SiloHostParts } from "@thresh/hosting/silo-host";
import type { ClusterNode } from "@thresh/runtime/cluster-node";

function buildHost(order: string[], onBeforeDeactivate: Array<() => Promise<void>>): SiloHost {
  const health = new HealthCheck();
  const node = {
    stop: async () => {
      order.push("node.stop (deactivateAll)");
    },
  } as unknown as ClusterNode;
  const shutdown = new GracefulShutdown(health, node, { delay: () => Promise.resolve() });
  const parts: SiloHostParts = {
    node,
    serviceId: "svc",
    health,
    healthServer: undefined,
    healthPort: undefined,
    shutdown,
    membership: {
      current: () => ({ silos: [], version: 0 }),
      updates: async function* () {},
    } as never,
    onBeforeDeactivate,
    onStop: [
      async () => {
        order.push("onStop");
      },
    ],
  };
  return new SiloHost(parts);
}

describe("SiloHost.stop ordering (issue #108)", () => {
  it("runs onBeforeDeactivate hooks (stream providers, durable-job manager) before the node deactivates activations", async () => {
    const order: string[] = [];
    const host = buildHost(order, [
      async () => {
        order.push("stream provider stop");
      },
      async () => {
        order.push("durable job manager stop");
      },
    ]);

    await host.stop();

    expect(order).toEqual([
      "stream provider stop",
      "durable job manager stop",
      "node.stop (deactivateAll)",
      "onStop",
    ]);
  });

  it("still drains the node and runs onStop when an onBeforeDeactivate hook throws", async () => {
    // These hooks now run FIRST: a provider whose stop() fails must not skip
    // deactivating every activation, closing transport and the onStop teardown
    // (Orleans logs a failed lifecycle stop and carries on stopping).
    const order: string[] = [];
    const host = buildHost(order, [
      async () => {
        throw new Error("provider stop failed");
      },
      async () => {
        order.push("durable job manager stop");
      },
    ]);

    await host.stop();

    expect(order).toEqual(["durable job manager stop", "node.stop (deactivateAll)", "onStop"]);
  });
});

describe("SiloHost hook failures are logged, not only thrown", () => {
  function hostWith(
    hooks: Partial<Pick<SiloHostParts, "onStart" | "onStop" | "startupTasks">>,
    throwingLogger = false,
  ): {
    host: SiloHost;
    errors: Array<{ message: string; fields?: LogFields }>;
  } {
    const errors: Array<{ message: string; fields?: LogFields }> = [];
    const logger: Logger = {
      debug: () => undefined,
      info: () => undefined,
      warn: () => undefined,
      error: (message, fields) => {
        if (throwingLogger) throw new Error("logger down");
        errors.push({ message, ...(fields !== undefined ? { fields } : {}) });
      },
    };
    const health = new HealthCheck();
    const node = {
      start: async () => undefined,
      stop: async () => undefined,
      recoverySettled: async () => undefined,
      onListenerClosed: () => undefined,
      onViewApplied: () => () => undefined,
      hasAppliedView: () => true,
      appliedViewVersion: 0,
      updateView: () => undefined,
      ownedHashRanges: () => [],
    } as unknown as ClusterNode;
    const host = new SiloHost({
      node,
      serviceId: "svc",
      health,
      healthServer: undefined,
      healthPort: undefined,
      shutdown: new GracefulShutdown(health, node, { delay: () => Promise.resolve() }),
      membership: {
        current: () => ({ silos: [], version: 0 }),
        updates: async function* () {},
      } as never,
      logger,
      ...hooks,
    });
    return { host, errors };
  }

  it("logs a start hook that throws, and still fails the start", async () => {
    const { host, errors } = hostWith({
      onStart: [
        async () => {
          throw new Error("connect failed");
        },
      ],
    });
    await expect(host.start()).rejects.toThrow("connect failed");
    expect(errors.map((e) => e.message)).toEqual(["silo start hook failed"]);
    expect(errors[0]!.fields).toMatchObject({ phase: "onStart" });
  });

  it("surfaces the original start error when the logger throws", async () => {
    const { host } = hostWith(
      {
        onStart: [
          async () => {
            throw new Error("connect failed");
          },
        ],
      },
      true,
    );
    await expect(host.start()).rejects.toThrow("connect failed");
  });

  it("logs a startup task that throws", async () => {
    const { host, errors } = hostWith({
      startupTasks: [
        async () => {
          throw new Error("warmup failed");
        },
      ],
    });
    await expect(host.start()).rejects.toThrow("warmup failed");
    expect(errors[0]!.fields).toMatchObject({ phase: "startupTasks" });
  });

  it("logs a stop hook that throws, and still fails the stop", async () => {
    const { host, errors } = hostWith({
      onStop: [
        async () => {
          throw new Error("disconnect failed");
        },
      ],
    });
    await expect(host.stop()).rejects.toThrow("disconnect failed");
    expect(errors.map((e) => e.message)).toEqual(["silo stop hook failed"]);
  });
});
