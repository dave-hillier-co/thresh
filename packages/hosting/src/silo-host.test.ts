import { describe, expect, it } from "vitest";
import type { LogFields, Logger } from "@thresh/core/logger";
import { ServiceLifecycleStage } from "@thresh/runtime/lifecycle-subject";
import { GracefulShutdown } from "@thresh/hosting/graceful-shutdown";
import { HealthCheck } from "@thresh/hosting/health-check";
import {
  SiloHost,
  type SiloHostParts,
  type SiloLifecycleParticipant,
} from "@thresh/hosting/silo-host";
import type { ClusterNode } from "@thresh/runtime/cluster-node";

function fakeNode(order: string[] = []): ClusterNode {
  return {
    start: async () => undefined,
    stop: async () => {
      order.push("node.stop (deactivateAll)");
    },
    recoverySettled: async () => undefined,
    onListenerClosed: () => undefined,
    onViewApplied: () => () => undefined,
    hasAppliedView: () => true,
    appliedViewVersion: 0,
    updateView: () => undefined,
    ownedHashRanges: () => [],
  } as unknown as ClusterNode;
}

function buildHost(order: string[], lifecycle: SiloLifecycleParticipant[]): SiloHost {
  const health = new HealthCheck();
  const node = fakeNode(order);
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
    lifecycle: [
      ...lifecycle,
      {
        name: "teardown",
        stage: ServiceLifecycleStage.RuntimeInitialize,
        stop: async () => {
          order.push("onStop");
        },
      },
    ],
  };
  return new SiloHost(parts);
}

describe("SiloHost.stop ordering (issue #108)", () => {
  it("stops participants below the node (stream providers, the durable-job manager) before the node deactivates activations", async () => {
    const order: string[] = [];
    const host = buildHost(order, [
      {
        name: "stream provider",
        stage: ServiceLifecycleStage.BecomeActive,
        stop: async () => void order.push("stream provider stop"),
      },
      {
        name: "durable job manager",
        stage: ServiceLifecycleStage.BecomeActive,
        stop: async () => void order.push("durable job manager stop"),
      },
    ]);

    await host.start();
    await host.stop();

    // Within one stage the stops run in the reverse of registration order.
    expect(order).toEqual([
      "durable job manager stop",
      "stream provider stop",
      "node.stop (deactivateAll)",
      "onStop",
    ]);
  });

  it("still drains the node and runs the teardown when a pre-deactivation stop throws", async () => {
    // A provider whose stop() fails must not skip deactivating every activation, closing
    // transport and the teardown (Orleans logs a failed lifecycle stop and carries on stopping).
    const order: string[] = [];
    const host = buildHost(order, [
      {
        name: "stream provider",
        stage: ServiceLifecycleStage.BecomeActive,
        stop: async () => {
          throw new Error("provider stop failed");
        },
      },
      {
        name: "durable job manager",
        stage: ServiceLifecycleStage.BecomeActive,
        stop: async () => void order.push("durable job manager stop"),
      },
    ]);

    await host.start();
    await host.stop();

    expect(order).toEqual(["durable job manager stop", "node.stop (deactivateAll)", "onStop"]);
  });
});

describe("SiloHost hook failures are logged, not only thrown", () => {
  function hostWith(
    hooks: Pick<SiloHostParts, "lifecycle" | "startupTasks">,
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
    const node = fakeNode();
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
      lifecycle: [
        {
          name: "connect",
          stage: ServiceLifecycleStage.RuntimeInitialize,
          start: async () => {
            throw new Error("connect failed");
          },
        },
      ],
    });
    await expect(host.start()).rejects.toThrow("connect failed");
    expect(errors.map((e) => e.message)).toEqual(["silo start hook failed"]);
    expect(errors[0]!.fields).toMatchObject({ phase: "connect" });
  });

  it("surfaces the original start error when the logger throws", async () => {
    const { host } = hostWith(
      {
        lifecycle: [
          {
            name: "connect",
            stage: ServiceLifecycleStage.RuntimeInitialize,
            start: async () => {
              throw new Error("connect failed");
            },
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

  it("logs a stop hook that throws, and completes the stop", async () => {
    const { host, errors } = hostWith({
      lifecycle: [
        {
          name: "disconnect",
          stage: ServiceLifecycleStage.RuntimeInitialize,
          stop: async () => {
            throw new Error("disconnect failed");
          },
        },
      ],
    });
    await host.start();
    await expect(host.stop()).resolves.toBeUndefined();
    expect(errors.map((e) => e.message)).toEqual(["silo stop hook failed"]);
    expect(errors[0]!.fields).toMatchObject({ phase: "disconnect" });
  });
});
