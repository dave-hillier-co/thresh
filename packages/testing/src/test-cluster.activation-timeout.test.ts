import { afterEach, describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import type { GrainType } from "@thresh/core/grain-type";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TestCluster } from "@thresh/testing/test-cluster";

/** While set, every `onActivate` of the grain below hangs for ever. */
let hang = true;
let activations = 0;

interface ISlowStarter extends GrainKey<string> {
  ping(): Promise<string>;
}
const ISlowStarter = defineGrainInterface<ISlowStarter>("ISlowStarter.activationTimeout");

@grain({ name: "ActivationTimeoutSlowStarter" })
class SlowStarterGrain extends Grain implements ISlowStarter {
  override async onActivate(): Promise<void> {
    activations += 1;
    if (hang) await new Promise<never>(() => {});
  }

  async ping(): Promise<string> {
    return "pong";
  }
}

const slowStarterId = (key: string) =>
  new GrainId("ActivationTimeoutSlowStarter" as GrainType, key);

const flush = async (): Promise<void> => {
  for (let i = 0; i < 10; i += 1) await new Promise((resolve) => setImmediate(resolve));
};

let clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters) await c.dispose();
  clusters = [];
  hang = true;
  activations = 0;
});

describe("an activation that does not finish within the activation timeout", () => {
  it("fails its calls, leaves the catalog, and a later call activates afresh", async () => {
    const time = new FakeTimeProvider();
    const cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: SlowStarterGrain, interfaces: [ISlowStarter] }],
      time,
      activationTimeout: { seconds: 5 },
    });
    clusters.push(cluster);
    const ref = cluster.getGrain(ISlowStarter, "stuck");

    const first = ref.ping();
    const second = ref.ping();
    first.catch(() => undefined);
    second.catch(() => undefined);
    await flush();
    expect(activations).toBe(1);

    time.advance(4_999);
    await flush();
    let settled = false;
    void first.then(
      () => (settled = true),
      () => (settled = true),
    );
    await flush();
    expect(settled).toBe(false); // still inside the timeout

    time.advance(1);
    await expect(first).rejects.toThrow(/activation/i);
    await expect(second).rejects.toThrow(/activation/i);
    await flush();
    expect(cluster.primary.host.isActive(slowStarterId("stuck"))).toBe(false);

    hang = false;
    await expect(ref.ping()).resolves.toBe("pong");
    expect(activations).toBe(2);
  });
});
