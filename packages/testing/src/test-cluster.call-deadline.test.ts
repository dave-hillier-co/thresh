import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { GrainCallTimeoutError } from "@thresh/core/errors";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { TestCluster, type TestClusterOptions } from "@thresh/testing/test-cluster";

/**
 * One call deadline for every call (Orleans applies `ResponseTimeout` to local and remote
 * targets alike). A same-silo call has the cross-silo call timeout with nothing configured; a
 * method's own response timeout and the silo default still override it.
 */
let release: () => void = () => {};
const stalled = new Promise<void>((resolve) => {
  release = resolve;
});

const Ping = defineGrain(
  "CallDeadlinePing",
  (ctx) => ({
    ping: async (): Promise<string> => ctx.getGrain(Pong, "b").pong(String(ctx.id.key)),
    pingQuickly: async (): Promise<string> => ctx.getGrain(Pong, "b").pong(String(ctx.id.key)),
    again: async (): Promise<string> => "again",
    stall: async (): Promise<void> => stalled,
  }),
  {
    interfaceOptions: {
      stall: { oneWay: true },
      pingQuickly: { responseTimeout: { seconds: 1 } },
    },
  },
);
const Pong = defineGrain("CallDeadlinePong", (ctx) => ({
  pong: async (selfKey: string): Promise<string> => ctx.getGrain(Ping, selfKey).again(),
}));

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.dispose();
});

async function start(
  time: FakeTimeProvider,
  options: Partial<TestClusterOptions> = {},
): Promise<TestCluster> {
  const cluster = await TestCluster.start({
    initialSilos: 1,
    grains: [Ping, Pong],
    reminders: false,
    time,
    ...options,
  });
  clusters.push(cluster);
  return cluster;
}

describe("a same-silo call's default deadline", () => {
  it("rejects a co-located A -> B -> A cycle after the default call timeout", async () => {
    const time = new FakeTimeProvider();
    const cluster = await start(time);
    const outcome = cluster.primary.host
      .getGrain(Ping, "a")
      .ping()
      .catch((e: unknown) => e);
    await new Promise((r) => setTimeout(r, 20));
    time.advance(29_999);
    await new Promise((r) => setTimeout(r, 20));
    let settled = false;
    void outcome.then(() => (settled = true));
    await new Promise((r) => setTimeout(r, 5));
    expect(settled).toBe(false);
    time.advance(1);
    expect(await outcome).toBeInstanceOf(GrainCallTimeoutError);
  });

  it("is the silo's callTimeout when one is set", async () => {
    const time = new FakeTimeProvider();
    const cluster = await start(time, { callTimeout: { seconds: 2 } });
    const outcome = cluster.primary.host
      .getGrain(Ping, "a2")
      .ping()
      .catch((e: unknown) => e);
    await new Promise((r) => setTimeout(r, 20));
    time.advance(2_000);
    expect(await outcome).toBeInstanceOf(GrainCallTimeoutError);
  });

  it("yields to the silo default response timeout", async () => {
    const time = new FakeTimeProvider();
    const cluster = await start(time, { defaultResponseTimeout: { seconds: 3 } });
    const outcome = cluster.primary.host
      .getGrain(Ping, "a3")
      .ping()
      .catch((e: unknown) => e);
    await new Promise((r) => setTimeout(r, 20));
    time.advance(3_000);
    expect(await outcome).toBeInstanceOf(GrainCallTimeoutError);
  });

  it("yields to the method's own response timeout", async () => {
    const time = new FakeTimeProvider();
    const cluster = await start(time);
    const outcome = cluster.primary.host
      .getGrain(Ping, "a5")
      .pingQuickly()
      .catch((e: unknown) => e);
    await new Promise((r) => setTimeout(r, 20));
    time.advance(1_000);
    expect(await outcome).toBeInstanceOf(GrainCallTimeoutError);
    // Let the abandoned cycle unwind so the cluster can stop.
    time.advance(30_000);
    await new Promise((r) => setTimeout(r, 20));
  });

  it("does not race a one-way call", async () => {
    const time = new FakeTimeProvider();
    const cluster = await start(time);
    const outcome = cluster.primary.host
      .getGrain(Ping, "a4")
      .stall()
      .then(
        () => "resolved",
        (e: unknown) => e,
      );
    await new Promise((r) => setTimeout(r, 20));
    time.advance(60_000);
    release();
    expect(await outcome).not.toBeInstanceOf(GrainCallTimeoutError);
  });
});
