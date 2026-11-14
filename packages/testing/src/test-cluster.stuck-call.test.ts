import { afterEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { GrainType } from "@thresh/core/grain-type";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

/**
 * A call that got stuck while executing must run once and surface to its caller, never be
 * resent; calls queued behind it never ran and are rerouted to a fresh activation.
 */

let starts: Record<string, number> = {};
let release: () => void = () => undefined;
let released: Promise<void> = Promise.resolve();

function rearm(): void {
  released = new Promise<void>((resolve) => (release = resolve));
}

interface IHanger extends GrainKey<string> {
  /** Hangs until the test releases it. */
  hang(): Promise<string>;
  /** Hangs on a call to itself (a non-reentrant self-call deadlock). */
  hangOnSelf(): Promise<string>;
  ping(): Promise<string>;
}
const IHanger = defineGrainInterface<IHanger>("IHanger.stuckCall");

@grain({ name: "StuckCallHanger" })
class HangerGrain extends Grain implements IHanger {
  async hang(): Promise<string> {
    this.started("hang");
    await released;
    return "done";
  }

  async hangOnSelf(): Promise<string> {
    this.started("hangOnSelf");
    return this.getGrain(IHanger, String(this.id.key)).ping();
  }

  async ping(): Promise<string> {
    return "pong";
  }

  private started(method: string): void {
    const key = `${this.id.key}:${method}`;
    starts[key] = (starts[key] ?? 0) + 1;
  }
}

const hangerId = (key: string) => new GrainId("StuckCallHanger" as GrainType, key);

const flush = async (): Promise<void> => {
  for (let i = 0; i < 20; i += 1) await new Promise((resolve) => setImmediate(resolve));
};

let clusters: TestCluster[] = [];
afterEach(async () => {
  release();
  for (const c of clusters) await c.dispose();
  clusters = [];
  starts = {};
});

const MAX_REQUEST_PROCESSING_MS = 1_000;

async function start(time: FakeTimeProvider, path: Path): Promise<TestCluster> {
  const cluster = await TestCluster.start({
    initialSilos: 2,
    grains: [{ ctor: HangerGrain, interfaces: [IHanger] }],
    time,
    maxRequestProcessingTime: { ms: MAX_REQUEST_PROCESSING_MS },
    defaultPlacementStrategy: new PreferLocalPlacement(),
    // Without the codec round trip a same-silo nested call's error reaches its caller as the
    // very object the activation refused it with.
    wireSemanticsForLocalCalls: path !== "same-silo, shared objects",
  });
  clusters.push(cluster);
  return cluster;
}

type Path = "same-silo" | "same-silo, shared objects" | "cross-silo" | "external client";

/** Who calls, for a grain the call first activates on `host` (prefer-local placement). */
async function callerFor(
  cluster: TestCluster,
  path: Path,
): Promise<{ via: (key: string) => IHanger; host: TestSiloHandle }> {
  const [first, second] = cluster.silos as [TestSiloHandle, TestSiloHandle];
  switch (path) {
    case "same-silo":
    case "same-silo, shared objects":
      return { via: (key) => first.host.getGrain(IHanger, key), host: first };
    case "cross-silo": {
      // Activate on the second silo, then call it from the first.
      return { via: (key) => first.host.getGrain(IHanger, key), host: second };
    }
    case "external client": {
      const client = await cluster.client;
      return { via: (key) => client.getGrain(IHanger, key), host: first };
    }
  }
}

describe.each<Path>(["same-silo", "same-silo, shared objects", "cross-silo", "external client"])(
  "a call stuck while executing, %s",
  (path) => {
    it("runs once, and the call queued behind it is rerouted", async () => {
      rearm();
      const time = new FakeTimeProvider();
      const cluster = await start(time, path);
      const key = `hang-${path}`;
      const { via, host } = await callerFor(cluster, path);
      // Place the activation on `host`.
      await host.host.getGrain(IHanger, key).ping();
      expect(cluster.silos.some((s) => s.host.isActive(hangerId(key)))).toBe(true);
      if (path !== "external client") expect(host.host.isActive(hangerId(key))).toBe(true);

      const stuck = via(key).hang();
      let stuckOutcome: unknown;
      stuck.then(
        (v) => (stuckOutcome = v),
        (e: unknown) => (stuckOutcome = e),
      );
      await flush();
      expect(starts[`${key}:hang`]).toBe(1);

      time.advance(MAX_REQUEST_PROCESSING_MS);
      const queued = via(key).ping(); // waits behind the overdue turn: declares it stuck
      await expect(queued).resolves.toBe("pong");
      await flush();
      expect(starts[`${key}:hang`]).toBe(1);

      release();
      await flush();
      expect(stuckOutcome).toBe("done");
      expect(starts[`${key}:hang`]).toBe(1);
    });

    it("runs once when it is stuck on a call to itself", async () => {
      const time = new FakeTimeProvider();
      const cluster = await start(time, path);
      const key = `self-${path}`;
      const { via, host } = await callerFor(cluster, path);
      await host.host.getGrain(IHanger, key).ping();

      const stuck = via(key).hangOnSelf();
      let stuckOutcome: unknown;
      stuck.then(
        (v) => (stuckOutcome = v),
        (e: unknown) => (stuckOutcome = e),
      );
      await flush();
      expect(starts[`${key}:hangOnSelf`]).toBe(1);

      // Its self-call waits behind it: once overdue, the activation is declared stuck.
      time.advance(MAX_REQUEST_PROCESSING_MS);
      await flush();
      time.advance(1);
      await flush();

      expect(starts[`${key}:hangOnSelf`]).toBe(1);
      // The self-call never ran on the stuck activation: rerouted to a fresh one, it answered.
      expect(stuckOutcome).toBe("pong");
    });
  },
);
