import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { RejectionError } from "@thresh/core/errors";
import type { Logger } from "@thresh/core/logger";
import { NetworkFaults } from "@thresh/messaging/fault-injecting-transport";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import {
  TestCluster,
  type TestClusterOptions,
  type TestSiloHandle,
} from "@thresh/testing/test-cluster";

/**
 * M9: a reply travels down the connection its request arrived on, between silos as well as to
 * clients, and losing that connection fails exactly the calls pending on it.
 */
const gates = new Map<string, { started: () => void; release: () => void; wait: Promise<void> }>();
const startedSignals = new Map<string, Promise<void>>();

function gate(key: string): void {
  let started!: () => void;
  let release!: () => void;
  startedSignals.set(key, new Promise((resolve) => (started = resolve)));
  const wait = new Promise<void>((resolve) => (release = resolve));
  gates.set(key, { started, release, wait });
}

const Echo = defineGrain("ReplyPathEcho", () => ({
  echo: async (value: string): Promise<string> => value,
  held: async (key: string): Promise<string> => {
    const g = gates.get(key);
    if (g !== undefined) {
      g.started();
      await g.wait;
    }
    return key;
  },
}));

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const g of gates.values()) g.release();
  gates.clear();
  for (const c of clusters.splice(0)) await c.dispose();
});

async function start(options: Partial<TestClusterOptions> = {}): Promise<TestCluster> {
  const cluster = await TestCluster.start({
    initialSilos: 3,
    grains: [Echo],
    defaultPlacementStrategy: new PreferLocalPlacement(),
    callTimeout: { ms: 2_000 },
    reminders: false,
    ...options,
  });
  clusters.push(cluster);
  return cluster;
}

/** Activate a grain on `home` and return its key; skips keys whose activation `home` cannot complete. */
async function placeOn(home: TestSiloHandle, prefix: string): Promise<string> {
  for (let i = 0; i < 32; i++) {
    const key = `${prefix}-${i}`;
    const placed = await home.host
      .getGrain(Echo, key)
      .echo("placed")
      .catch(() => undefined);
    if (placed === "placed") return key;
  }
  throw new Error(`no key could be activated on ${home.address.toString()}`);
}

describe("replies between silos", () => {
  it("answer down the request's connection, so a call succeeds when the reverse direction cannot be dialled", async () => {
    // Set before the cluster starts: silo 1 can never open a connection to silo 0.
    const faults = new NetworkFaults();
    faults.failConnects({ from: "test-silo-1", to: "test-silo-0" });
    const cluster = await start({ faults });
    const [a, b] = cluster.silos as [TestSiloHandle, TestSiloHandle];
    const key = await placeOn(b, "on-b");

    await expect(a.host.getGrain(Echo, key).echo("across")).resolves.toBe("across");
  });

  it("fail the calls pending on a lost connection promptly, and touch no other call", async () => {
    const warnings: string[] = [];
    const logger: Logger = {
      debug() {},
      info() {},
      warn: (message) => void warnings.push(message),
      error() {},
    };
    const cluster = await start({ logger });
    const [a, b, c] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const onB = await placeOn(b, "on-b");
    const onC = await placeOn(c, "on-c");
    // Fill a's location cache, so the held calls below cross exactly one link each.
    await a.host.getGrain(Echo, onB).echo("warm");
    await a.host.getGrain(Echo, onC).echo("warm");
    gate(onB);
    gate(onC);

    const toB = a.host.getGrain(Echo, onB).held(onB);
    const toC = a.host.getGrain(Echo, onC).held(onC);
    await startedSignals.get(onB);
    await startedSignals.get(onC);

    const startedAt = Date.now();
    expect(await cluster.faults.sever({ from: a.address, to: b.address })).toBe(1);

    const error = await toB.catch((e: unknown) => e);
    expect(error).toBeInstanceOf(RejectionError);
    expect((error as RejectionError).kind).toBe("siloUnavailable");
    expect(Date.now() - startedAt).toBeLessThan(1_000);

    // The call to c is still waiting for its own reply, and gets it.
    gates.get(onC)!.release();
    await expect(toC).resolves.toBe(onC);

    // b's turn still finishes; its reply has nowhere to go, which is logged rather than lost.
    gates.get(onB)!.release();
    for (let i = 0; i < 50 && !warnings.some((w) => /reply/.test(w)); i++) {
      await new Promise((resolve) => setTimeout(resolve, 5));
    }
    expect(warnings.some((w) => /reply/.test(w))).toBe(true);
  });
});
