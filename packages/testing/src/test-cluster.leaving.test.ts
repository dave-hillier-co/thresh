import { afterEach, describe, expect, it } from "vitest";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { LogFields, Logger } from "@thresh/core/logger";
import type { GrainDirectoryPartition } from "@thresh/directory/grain-directory-partition";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import {
  TestCluster,
  type StopSiloOptions,
  type TestClusterOptions,
  type TestSiloHandle,
} from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";

/**
 * A silo leaving the cluster (docs/design-membership-and-directory.md, "Leaving"): it follows
 * the watch through its stop, hands its ranges over by snapshot while it still serves, waits for
 * the new owners to collect them, and only then deactivates, unregistering at the new owners.
 * Calls are issued without waiting for the cluster to settle, and the directory's integrity check
 * runs after every step.
 */

/** Every deactivation hook that ran, by the silo it ran on. */
let deactivations: Array<{ silo: string; key: string; at: number }> = [];
/** The pod whose deactivation hooks are slow, and how slow. */
let slow: { silo: string; ms: number } | undefined;

interface ILeavingCounter extends GrainKey<string> {
  inc(): Promise<number>;
}
const ILeavingCounter = defineGrainInterface<ILeavingCounter>("ILeavingCounter");

@grain({ name: "LeavingCounter" })
class LeavingCounter extends Grain implements ILeavingCounter {
  private n = 0;

  async inc(): Promise<number> {
    return ++this.n;
  }

  override async onDeactivate(): Promise<void> {
    const silo = this.runtime.localSiloAddress().podName;
    deactivations.push({ silo, key: String(this.id.key), at: performance.now() });
    if (slow?.silo === silo) await sleep(slow.ms);
    if (witness !== undefined && witness.silo !== silo) {
      try {
        await this.getGrain(ILeavingWitness, "witness").saw(String(this.id.key));
      } catch (error) {
        witness.failures.push(`${String(this.id.key)} on ${silo}: ${String(error)}`);
      }
    }
  }
}

/**
 * When set, every deactivation hook on a silo other than `silo` reports to the witness grain,
 * which lives on `silo`: what reached it, and every report that failed.
 */
let witness: { silo: string; seen: string[]; failures: string[] } | undefined;

interface ILeavingWitness extends GrainKey<string> {
  saw(key: string): Promise<void>;
}
const ILeavingWitness = defineGrainInterface<ILeavingWitness>("ILeavingWitness");

@grain({ name: "LeavingWitness" })
class LeavingWitness extends Grain implements ILeavingWitness {
  async saw(key: string): Promise<void> {
    witness?.seen.push(key);
  }
}

const TYPE = "LeavingCounter";
const id = (key: string): GrainId => new GrainId(TYPE, key);

const clusters: TestCluster[] = [];
afterEach(async () => {
  slow = undefined;
  for (const c of clusters.splice(0)) await c.dispose();
  deactivations = [];
  witness = undefined;
});

async function start(options: Partial<TestClusterOptions> = {}): Promise<TestCluster> {
  const cluster = await TestCluster.start({
    membership: "kubernetes",
    grains: [{ ctor: LeavingCounter, interfaces: [ILeavingCounter] }],
    reminders: false,
    callTimeout: { seconds: 2 },
    ...options,
  });
  clusters.push(cluster);
  return cluster;
}

// Each silo hosts the grains first called through it, so the leaver hosts some.
const preferLocal = { defaultPlacementStrategy: new PreferLocalPlacement() };

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const partitionOf = (silo: TestSiloHandle): GrainDirectoryPartition =>
  (silo.host as unknown as { parts: { node: { partition: GrainDirectoryPartition } } }).parts.node
    .partition;

const isFenced = (silo: TestSiloHandle): boolean =>
  (silo.host as unknown as { parts: { node: { isFenced: boolean } } }).parts.node.isFenced;

/** Every value each key's calls returned, and every call that failed. */
class Results {
  readonly values = new Map<string, number[]>();
  readonly errors: unknown[] = [];

  record(key: string, value: number): void {
    const list = this.values.get(key) ?? [];
    list.push(value);
    this.values.set(key, list);
  }

  /** Keys whose counter answered the same value twice: two activations, or one restarted. */
  restarted(keys: Iterable<string> = this.values.keys()): string[] {
    const out: string[] = [];
    for (const key of keys) {
      const list = this.values.get(key) ?? [];
      if (new Set(list).size !== list.length) out.push(key);
    }
    return out;
  }
}

async function call(results: Results, silo: TestSiloHandle, key: string): Promise<void> {
  try {
    results.record(key, await silo.host.getGrain(ILeavingCounter, key).inc());
  } catch (error) {
    results.errors.push(error);
  }
}

/** Calls in flight throughout: every key, round after round, through one of `silos()`, rotating. */
class Load {
  private running = true;
  private readonly loop: Promise<void>;

  constructor(
    private readonly results: Results,
    private readonly keys: () => readonly string[],
    private readonly silos: () => readonly TestSiloHandle[],
  ) {
    this.loop = this.run();
  }

  private async run(): Promise<void> {
    for (let round = 0; this.running; round++) {
      const silos = this.silos();
      if (silos.length > 0) {
        await Promise.all(
          this.keys().map((key, i) => call(this.results, silos[(i + round) % silos.length]!, key)),
        );
      }
      await sleep(1);
    }
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.loop;
  }
}

/**
 * The load during a drain. Every key is called through the survivors throughout; the survivors'
 * keys are also called through the leaver until its deactivation starts. (A call the leaver
 * itself is still waiting on when it closes is rejected with it, as on any stopping process, so
 * the leaver calls only grains it does not host.)
 */
class DrainLoad {
  readonly deactivationStarted: Promise<void>;
  private readonly throughSurvivors: Load;

  constructor(
    results: Results,
    survivors: readonly TestSiloHandle[],
    leaver: TestSiloHandle,
    leaverCalls: readonly string[],
    keys: readonly string[],
  ) {
    this.throughSurvivors = new Load(
      results,
      () => keys,
      () => survivors,
    );
    const throughLeaver = new Load(
      results,
      () => leaverCalls,
      () => [leaver],
    );
    this.deactivationStarted = (async () => {
      await waitFor(() => deactivations.some((d) => d.silo === leaver.address.podName), {
        timeoutMs: 20_000,
        intervalMs: 1,
      });
      await throughLeaver.stop();
    })();
  }

  async stop(): Promise<void> {
    await this.deactivationStarted;
    await this.throughSurvivors.stop();
  }
}

function activeTwice(cluster: TestCluster, keys: Iterable<string>): string[] {
  const out: string[] = [];
  for (const key of keys) {
    if (cluster.silos.filter((s) => s.host.isActive(id(key))).length > 1) out.push(key);
  }
  return out;
}

const keysFor = (prefix: string, n: number): string[] =>
  Array.from({ length: n }, (_, i) => `${prefix}-${i}`);

const statsOf = (silos: readonly TestSiloHandle[]): { transfers: number; recoveries: number } =>
  silos.reduce(
    (sum, s) => ({
      transfers: sum.transfers + partitionOf(s).stats.transfers,
      recoveries: sum.recoveries + partitionOf(s).stats.recoveries,
    }),
    { transfers: 0, recoveries: 0 },
  );

/** Keys whose entry at some survivor's partition names `leaver`. */
function entriesNaming(
  survivors: readonly TestSiloHandle[],
  leaver: TestSiloHandle,
  keys: readonly string[],
): string[] {
  return keys.filter((key) =>
    survivors.some((s) => partitionOf(s).entryFor(id(key))?.silo.equals(leaver.address) === true),
  );
}

/** The warnings every silo logged: a stop whose handover bound elapsed logs one. */
class WarningLogger implements Logger {
  readonly warnings: Array<{ message: string; fields?: LogFields }> = [];
  debug(): void {}
  info(): void {}
  warn(message: string, fields?: LogFields): void {
    this.warnings.push({ message, ...(fields ? { fields } : {}) });
  }
  error(): void {}

  /** The stops that spent their whole handover bound, and who they were still waiting for. */
  handoversIncomplete(): unknown[] {
    return this.warnings
      .filter((w) => w.message.startsWith("directory handover incomplete"))
      .map((w) => w.fields);
  }
}

/** Three silos, each hosting `perSilo` counters called once through it. */
async function seeded(
  cluster: TestCluster,
  results: Results,
  perSilo: number,
): Promise<{ survivorKeys: string[]; leaverKeys: string[] }> {
  const [a, b, leaver] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
  const survivorKeys = [...keysFor("on-a", perSilo), ...keysFor("on-b", perSilo)];
  const leaverKeys = keysFor("on-leaver", perSilo);
  for (const key of keysFor("on-a", perSilo)) await call(results, a, key);
  for (const key of keysFor("on-b", perSilo)) await call(results, b, key);
  for (const key of leaverKeys) await call(results, leaver, key);
  expect(leaverKeys.every((key) => leaver.host.isActive(id(key)))).toBe(true);
  return { survivorKeys, leaverKeys };
}

describe("a leaving silo on Kubernetes membership", () => {
  it("applies the view in which it is terminating by itself, when the pod is reported terminating after its stop began", async () => {
    const cluster = await start({ initialSilos: 3, ...preferLocal });
    const [a, b, leaver] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    await seeded(cluster, results, 10);
    await cluster.checkIntegrity();

    // No peer can ask the leaver for anything: every peer's watch is held.
    cluster.kubernetes.hold(a);
    cluster.kubernetes.hold(b);
    const stopping = leaver.host.stop();
    await Promise.resolve();
    cluster.kubernetes.api.setTerminating(leaver.address.podName);
    await waitFor(() =>
      cluster
        .viewOf(leaver)
        .membership.silos.some((m) => m.address.equals(leaver.address) && m.status === "draining"),
    );
    await sleep(20);

    // Its partition has applied the view its own watch brought, and holds the snapshot of the
    // ranges it gave up.
    expect(partitionOf(leaver).view.version).toBe(cluster.viewOf(leaver).membership.version);
    expect(partitionOf(leaver).view.inRing(leaver.address)).toBe(false);
    expect(partitionOf(leaver).snapshotCount).toBeGreaterThan(0);

    await cluster.kubernetes.release(a);
    await cluster.kubernetes.release(b);
    await stopping;
    await cluster.stopSilo(leaver);
    await cluster.checkIntegrity();
    expect(results.errors).toEqual([]);
  }, 60_000);

  it("waits for the new owners to collect its snapshots before it deactivates", async () => {
    const cluster = await start({ initialSilos: 3, ...preferLocal });
    const [a, b, leaver] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const { leaverKeys } = await seeded(cluster, results, 10);
    await cluster.checkIntegrity();
    const before = statsOf([a, b]);

    cluster.kubernetes.hold(a);
    cluster.kubernetes.hold(b);
    let stopped = false;
    const stopping = cluster.stopSilo(leaver, { keepListed: true }).then(() => (stopped = true));
    await waitFor(() => partitionOf(leaver).snapshotCount > 0);
    await sleep(150);
    // Its snapshots are uncollected: nothing has been deactivated, and it still serves.
    expect(deactivations.filter((d) => d.silo === leaver.address.podName)).toEqual([]);
    expect(stopped).toBe(false);
    expect(leaverKeys.every((key) => leaver.host.isActive(id(key)))).toBe(true);

    // The owners see the change, collect, and deactivation proceeds.
    await cluster.kubernetes.release(a);
    await cluster.kubernetes.release(b);
    await stopping;
    expect(partitionOf(leaver).snapshotCount).toBe(0);
    expect(
      deactivations.filter((d) => d.silo === leaver.address.podName).map((d) => d.key),
    ).toEqual(expect.arrayContaining(leaverKeys));
    expect(statsOf([a, b]).transfers).toBeGreaterThan(before.transfers);
    expect(statsOf([a, b]).recoveries).toBe(before.recoveries);
    expect(entriesNaming([a, b], leaver, leaverKeys)).toEqual([]);
    await cluster.checkIntegrity();
    await cluster.kubernetes.removePod(leaver);
    await cluster.checkIntegrity();
  }, 60_000);

  it("starts deactivating when half the stop budget is spent on owners that never collect, and stops inside the budget", async () => {
    const stopBudgetMs = 600;
    const cluster = await start({ initialSilos: 3, stopBudgetMs, ...preferLocal });
    const [a, b, leaver] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const { leaverKeys } = await seeded(cluster, results, 5);
    await cluster.checkIntegrity();

    // The owners never see the change, so never collect.
    cluster.kubernetes.hold(a);
    cluster.kubernetes.hold(b);
    cluster.kubernetes.api.setTerminating(leaver.address.podName);
    await waitFor(() => partitionOf(leaver).snapshotCount > 0);
    const startedAt = performance.now();
    await cluster.stopSilo(leaver, { keepListed: true });
    const stoppedAfter = performance.now() - startedAt;

    const hooks = deactivations.filter((d) => d.silo === leaver.address.podName);
    expect(hooks.map((d) => d.key)).toEqual(expect.arrayContaining(leaverKeys));
    // TestCluster's grace is 0, so the whole budget remains after it: the wait gets half of it.
    const firstHookAfter = Math.min(...hooks.map((d) => d.at)) - startedAt;
    expect(firstHookAfter).toBeGreaterThanOrEqual(stopBudgetMs / 2 - 5);
    expect(stoppedAfter).toBeLessThan(stopBudgetMs + 250);
    expect(partitionOf(leaver).snapshotCount).toBeGreaterThan(0);

    await cluster.kubernetes.release(a);
    await cluster.kubernetes.release(b);
    await cluster.kubernetes.removePod(leaver);
    await cluster.checkIntegrity();
  }, 60_000);

  for (const terminating of ["beforeStop", "afterStopBegins"] as const) {
    it(`drains under load with no failed call and no recovery (pod terminating ${terminating})`, async () => {
      const cluster = await start({ initialSilos: 3, ...preferLocal });
      const [a, b, leaver] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
      const results = new Results();
      const { survivorKeys, leaverKeys } = await seeded(cluster, results, 15);
      await cluster.checkIntegrity();
      const before = statsOf([a, b]);
      slow = { silo: leaver.address.podName, ms: 1000 };

      const keys = [...survivorKeys, ...leaverKeys];
      const load = new DrainLoad(results, [a, b], leaver, survivorKeys, keys);
      await sleep(20);

      // Calls through the survivors to grains nobody has seen yet must not wait for its slow
      // deactivation hooks.
      const fresh = keysFor("fresh", 20);
      const latencies: number[] = [];
      const probe = (async () => {
        await load.deactivationStarted;
        await Promise.all(
          fresh.map(async (key, i) => {
            const t = performance.now();
            await call(results, i % 2 === 0 ? a : b, key);
            latencies.push(performance.now() - t);
          }),
        );
      })();
      const options: StopSiloOptions = { terminating, keepListed: true };
      await cluster.stopSilo(leaver, options);
      await probe;

      // After the stop and before the pod is removed: no survivor holds an entry naming the
      // leaver, and integrity holds.
      expect(entriesNaming([a, b], leaver, keys)).toEqual([]);
      await cluster.checkIntegrity();
      await cluster.kubernetes.removePod(leaver);
      await sleep(20);
      await load.stop();
      await cluster.checkIntegrity();

      expect(results.errors).toEqual([]);
      expect(results.restarted(survivorKeys)).toEqual([]);
      expect(activeTwice(cluster, keys)).toEqual([]);
      expect(statsOf([a, b]).transfers).toBeGreaterThan(before.transfers);
      expect(statsOf([a, b]).recoveries).toBe(before.recoveries);
      expect(latencies).toHaveLength(fresh.length);
      expect(Math.max(...latencies)).toBeLessThan(500);
    }, 60_000);
  }

  it("abandons its activations without their hooks when it sees itself removed during the drain", async () => {
    const cluster = await start({ initialSilos: 3, ...preferLocal });
    const [a, b, leaver] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const { leaverKeys } = await seeded(cluster, results, 8);
    await cluster.checkIntegrity();

    cluster.kubernetes.hold(a);
    cluster.kubernetes.hold(b);
    const stopping = cluster.stopSilo(leaver, { keepListed: true });
    await waitFor(() => partitionOf(leaver).snapshotCount > 0);
    // Kubernetes removes the pod while it still waits for its snapshots to be collected.
    cluster.kubernetes.api.removePod(leaver.address.podName);
    await stopping;

    expect(isFenced(leaver)).toBe(true);
    expect(deactivations.filter((d) => d.silo === leaver.address.podName)).toEqual([]);
    expect(leaverKeys.some((key) => leaver.host.isActive(id(key)))).toBe(false);

    await cluster.kubernetes.release(a);
    await cluster.kubernetes.release(b);
    await cluster.checkIntegrity();
    for (const key of leaverKeys) expect(await a.host.getGrain(ILeavingCounter, key).inc()).toBe(1);
    await cluster.checkIntegrity();
  }, 60_000);
});

describe("a silo with nobody to hand over to", () => {
  it("is the last silo of a cluster: it waits for nothing and transfers nothing", async () => {
    const cluster = await start({ initialSilos: 1, stopBudgetMs: 10_000 });
    const [only] = cluster.silos as [TestSiloHandle];
    const results = new Results();
    for (const key of keysFor("only", 10)) await call(results, only, key);
    await cluster.checkIntegrity();
    const startedAt = performance.now();
    await cluster.stopSilo(only);
    expect(performance.now() - startedAt).toBeLessThan(1000);
    expect(partitionOf(only).snapshotCount).toBe(0);
    expect(partitionOf(only).stats.transfers).toBe(0);
    expect(results.errors).toEqual([]);
  }, 60_000);

  for (const membership of ["static", "kubernetes"] as const) {
    it(`is the last silo of a disposed cluster: dispose() stops the silos one at a time, each handing over to the rest (${membership})`, async () => {
      const logger = new WarningLogger();
      const cluster = await TestCluster.start({
        initialSilos: 3,
        membership,
        grains: [
          { ctor: LeavingCounter, interfaces: [ILeavingCounter] },
          { ctor: LeavingWitness, interfaces: [ILeavingWitness] },
        ],
        reminders: false,
        stopBudgetMs: 10_000,
        logger,
        ...preferLocal,
      });
      const silos = [...cluster.silos];
      const last = silos[2]!;
      const results = new Results();
      const keys = keysFor("together", 30);
      for (const [i, key] of keys.entries()) await call(results, silos[i % 3]!, key);
      // The witness lives on the last silo; no other silo has called it, so none holds its route.
      await last.host.getGrain(ILeavingWitness, "witness").saw("seed");
      witness = { silo: last.address.podName, seen: [], failures: [] };
      await cluster.checkIntegrity();
      const before = statsOf(silos);

      await cluster.dispose();

      // Every hook ran, and every hook on a silo that stopped before the last reached the
      // witness on it, which had not stopped yet.
      expect(deactivations.map((d) => d.key)).toEqual(expect.arrayContaining(keys));
      const reporting = deactivations.filter((d) => d.silo !== last.address.podName);
      expect(reporting.length).toBeGreaterThan(0);
      expect(witness.failures).toEqual([]);
      expect([...witness.seen].sort()).toEqual(reporting.map((d) => d.key).sort());
      // The silos that stopped first handed their ranges to the rest by snapshot.
      expect(statsOf(silos).transfers).toBeGreaterThan(before.transfers);
      expect(statsOf(silos).recoveries).toBe(before.recoveries);
      // No stop spent its bound; the last silo had nobody to hand over to and held nothing.
      expect(logger.handoversIncomplete()).toEqual([]);
      for (const silo of silos) expect(partitionOf(silo).snapshotCount).toBe(0);
      expect(cluster.silos).toEqual([]);
      expect(results.errors).toEqual([]);
    }, 60_000);
  }

  it("lets a process that starts a cluster and disposes it exit on its own", () => {
    const fixture = fileURLToPath(new URL("./dispose-exit.fixture.ts", import.meta.url));
    const viteNode = fileURLToPath(
      new URL("../../../node_modules/.bin/vite-node", import.meta.url),
    );
    const run = spawnSync(viteNode, [fixture], { encoding: "utf8", timeout: 30_000 });
    expect(run.error).toBeUndefined();
    expect(run.status).toBe(0);
    expect(run.stdout).toContain("disposed static and kubernetes clusters");
  }, 60_000);
});

describe("a cluster stopping while a dead pod is still listed not ready", () => {
  // Half of it is the handover bound: far longer than any of these stops may take.
  const stopBudgetMs = 20_000;

  /** Three silos hosting thirty grains, and a fourth killed with its pod left listed not ready. */
  async function withDeadPod(logger: Logger): Promise<{
    cluster: TestCluster;
    live: TestSiloHandle[];
    dead: TestSiloHandle;
  }> {
    const cluster = await start({ initialSilos: 4, stopBudgetMs, logger });
    const dead = cluster.silos[1]!;
    const results = new Results();
    for (const [i, key] of keysFor("dead-pod", 30).entries()) {
      await call(results, cluster.silos[i % 4]!, key);
    }
    expect(results.errors).toEqual([]);
    await cluster.killSilo(dead);
    await cluster.kubernetes.markNotReady(dead);
    await cluster.checkIntegrity();
    const live = [...cluster.silos];
    expect(cluster.kubernetes.listed(dead)).toBe(true);
    return { cluster, live, dead };
  }

  it("disposes promptly: no stop waits on the dead pod", async () => {
    const logger = new WarningLogger();
    const { cluster, live, dead } = await withDeadPod(logger);

    const startedAt = performance.now();
    await cluster.dispose();
    expect(performance.now() - startedAt).toBeLessThan(stopBudgetMs / 2 / 4);
    // No stop spent its bound, and a leaver held what it gave the dead pod without waiting on it.
    expect(logger.handoversIncomplete()).toEqual([]);
    for (const silo of live) expect(partitionOf(silo).awaitedPartners).toEqual([]);
    expect(
      live.some((silo) => partitionOf(silo).view.ringSilos.every((s) => s.equals(dead.address))),
    ).toBe(true);
    expect(live.some((silo) => partitionOf(silo).snapshotCount > 0)).toBe(true);
  }, 120_000);

  it("stops every silo promptly when every live pod is reported terminating at once", async () => {
    const logger = new WarningLogger();
    const { cluster, live, dead } = await withDeadPod(logger);

    // A Deployment deleted: every live pod is terminating at once, so no silo is ready and the
    // ring falls back to the dead pod.
    for (const silo of live) cluster.kubernetes.api.setTerminating(silo.address.podName);
    await cluster.kubernetes.settle();
    for (const silo of live) {
      expect(partitionOf(silo).view.ringSilos).toEqual([dead.address]);
      expect(partitionOf(silo).snapshotCount).toBeGreaterThan(0);
    }

    const stoppedAfter = await Promise.all(
      live.map(async (silo) => {
        const startedAt = performance.now();
        await cluster.stopSilo(silo, { keepListed: true });
        return performance.now() - startedAt;
      }),
    );
    for (const ms of stoppedAfter) expect(ms).toBeLessThan(stopBudgetMs / 2 / 4);
    expect(logger.handoversIncomplete()).toEqual([]);
    for (const silo of live) {
      expect(partitionOf(silo).awaitedPartners).toEqual([]);
      // Still held for the dead pod, had it come for it.
      expect(partitionOf(silo).snapshotCount).toBeGreaterThan(0);
    }
  }, 120_000);
});

describe("a leaving silo on the default (static) membership", () => {
  it("is marked draining by stopSilo, so its ranges move by snapshot under load", async () => {
    const cluster = await start({ initialSilos: 3, membership: "static", ...preferLocal });
    const [a, b, leaver] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const { survivorKeys, leaverKeys } = await seeded(cluster, results, 15);
    await cluster.checkIntegrity();
    const before = statsOf([a, b]);
    slow = { silo: leaver.address.podName, ms: 300 };

    const keys = [...survivorKeys, ...leaverKeys];
    const load = new DrainLoad(results, [a, b], leaver, survivorKeys, keys);
    await sleep(20);
    await cluster.stopSilo(leaver);
    await load.deactivationStarted;
    await cluster.checkIntegrity();
    await sleep(20);
    await load.stop();
    await cluster.checkIntegrity();

    expect(results.errors).toEqual([]);
    expect(results.restarted(survivorKeys)).toEqual([]);
    expect(activeTwice(cluster, keys)).toEqual([]);
    expect(statsOf([a, b]).transfers).toBeGreaterThan(before.transfers);
    expect(statsOf([a, b]).recoveries).toBe(before.recoveries);
  }, 60_000);

  it("a silo whose host is stopped directly, which nobody marks draining, waits for nothing", async () => {
    const cluster = await start({
      initialSilos: 3,
      membership: "static",
      stopBudgetMs: 10_000,
      ...preferLocal,
    });
    const [, , stopped] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    await seeded(cluster, results, 5);
    await cluster.checkIntegrity();

    const startedAt = performance.now();
    await stopped.host.stop();
    expect(performance.now() - startedAt).toBeLessThan(1000);
    // It never left the ring, so it released nothing and held no snapshot to wait on.
    expect(partitionOf(stopped).view.inRing(stopped.address)).toBe(true);
    expect(partitionOf(stopped).snapshotCount).toBe(0);
    expect(deactivations.filter((d) => d.silo === stopped.address.podName).length).toBeGreaterThan(
      0,
    );
    expect(results.errors).toEqual([]);
  }, 60_000);
});

/** Orleans `ElasticChaos`, bounded and seeded so a failure replays. */
function mulberry32(seed: number): () => number {
  let s = seed >>> 0;
  return () => {
    s = (s + 0x6d2b79f5) >>> 0;
    let t = s;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

describe("directory chaos with graceful stops in both orders", () => {
  for (const seed of [11, 23, 42, 2026]) {
    it(`keeps one activation per grain through 25 rounds, alternating when the pod is reported terminating (seed ${seed})`, async () => {
      const random = mulberry32(seed);
      const cluster = await start({ initialSilos: 2 });
      const results = new Results();
      const keys = keysFor("chaos", 40);
      const load = new Load(
        results,
        () => keys,
        () => cluster.silos,
      );
      const actions: string[] = [];
      let drains = 0;
      try {
        for (let round = 0; round < 25; round++) {
          const count = cluster.silos.length;
          const roll = random();
          const victim = (): TestSiloHandle => cluster.silos[Math.floor(random() * count)]!;
          if (count < 5 && (count <= 1 || roll < 0.4)) {
            actions.push("start");
            await cluster.startAdditionalSilo();
          } else if (roll < 0.7) {
            const silo = victim();
            const terminating = drains++ % 2 === 0 ? "beforeStop" : "afterStopBegins";
            actions.push(`drain(${terminating}) ${silo.address.podName}`);
            await cluster.stopSilo(silo, { terminating });
          } else {
            const silo = victim();
            actions.push(`crash ${silo.address.podName}`);
            await cluster.killSilo(silo);
            await cluster.kubernetes.depart(silo);
          }
          await cluster.checkIntegrity().catch((error: unknown) => {
            throw new Error(`round ${round} (${actions.join(", ")}): ${String(error)}`);
          });
          expect(activeTwice(cluster, keys), `round ${round}: ${actions.join(", ")}`).toEqual([]);
        }
      } finally {
        await load.stop();
      }
      await cluster.checkIntegrity();
      expect(activeTwice(cluster, keys)).toEqual([]);
      expect([...results.values.values()].flat().length).toBeGreaterThan(keys.length * 5);
      expect(actions.some((a) => a.startsWith("drain(afterStopBegins)"))).toBe(true);
    }, 300_000);
  }
});
