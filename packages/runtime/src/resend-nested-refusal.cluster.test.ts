import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { RejectionError } from "@thresh/core/errors";
import { NetworkFaults } from "@thresh/messaging/fault-injecting-transport";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

/**
 * W6: "refused before execution" belongs to the refusal of one request, not to the error object
 * that described it. A grain method whose nested call is refused, and that lets that refusal
 * escape, has run: its own caller must not resend it — over a silo hop or from a client.
 */
const runs = new Map<string, number>();

const Target = defineGrain("NestedRefusalTarget", () => ({
  echo: async (value: string): Promise<string> => value,
}));

const Forwarder = defineGrain("NestedRefusalForwarder", (ctx) => ({
  echo: async (value: string): Promise<string> => value,
  forward: async (targetKey: string): Promise<string> => {
    const key = ctx.id.key as string;
    runs.set(key, (runs.get(key) ?? 0) + 1);
    // The nested call's refusal is rethrown as it is: the object the runtime refused it with.
    return ctx.getGrain(Target, targetKey).echo("nested");
  },
}));

const clusters: TestCluster[] = [];
afterEach(async () => {
  runs.clear();
  for (const c of clusters.splice(0)) await c.dispose();
});

async function placeOn(
  home: TestSiloHandle,
  grain: typeof Target | typeof Forwarder,
  prefix: string,
): Promise<string> {
  for (let i = 0; i < 32; i++) {
    const key = `${prefix}-${i}`;
    const placed = await home.host
      .getGrain(grain, key)
      .echo("placed")
      .catch(() => undefined);
    if (placed === "placed") return key;
  }
  throw new Error(`no key could be activated on ${home.address.toString()}`);
}

/**
 * Three silos: the forwarder lives on b, its target on c, and b can no longer reach c, so every
 * nested call the forwarder makes is refused before it runs (it never leaves b).
 */
async function setUp(): Promise<{
  cluster: TestCluster;
  a: TestSiloHandle;
  forwarder: string;
  target: string;
}> {
  const faults = new NetworkFaults();
  const cluster = await TestCluster.start({
    initialSilos: 3,
    grains: [Target, Forwarder],
    defaultPlacementStrategy: new PreferLocalPlacement(),
    callTimeout: { ms: 2_000 },
    reminders: false,
    faults,
  });
  clusters.push(cluster);
  const [a, b, c] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
  const target = await placeOn(c, Target, "target");
  const forwarder = await placeOn(b, Forwarder, "forwarder");
  // Warm a's route to the forwarder, then cut b off from c.
  await a.host.getGrain(Forwarder, forwarder).echo("warm");
  faults.failConnects({ from: b.address, to: c.address });
  await faults.sever({ from: b.address, to: c.address });
  return { cluster, a, forwarder, target };
}

describe("a grain that rethrows a nested call's refusal (W6)", () => {
  it("is not resent by a calling silo: the method runs once", async () => {
    const { a, forwarder, target } = await setUp();

    const error = await a.host
      .getGrain(Forwarder, forwarder)
      .forward(target)
      .catch((e: unknown) => e);

    expect(error).toBeInstanceOf(RejectionError);
    expect(runs.get(forwarder)).toBe(1);
  });

  it("is not resent by a client: the method runs once", async () => {
    const { cluster, forwarder, target } = await setUp();
    const client = await cluster.client;

    const error = await client
      .getGrain(Forwarder, forwarder)
      .forward(target)
      .catch((e: unknown) => e);

    expect(error).toBeInstanceOf(RejectionError);
    expect(runs.get(forwarder)).toBe(1);
  });
});
