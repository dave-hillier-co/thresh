import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { RejectionError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import type { Logger, LogFields } from "@thresh/core/logger";
import type { GrainDirectoryPartition } from "@thresh/directory/grain-directory-partition";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import {
  TestCluster,
  type TestClusterOptions,
  type TestSiloHandle,
} from "@thresh/testing/test-cluster";

/**
 * The directory partition protocol on the production membership path: every silo follows its
 * own watch of a fake EndpointSlice API, calls are issued without waiting for the cluster to
 * settle, and the directory's integrity check runs after every step.
 */
const Counter = defineGrain("DirectoryCounter", () => {
  let n = 0;
  return { inc: async (): Promise<number> => ++n };
});
const TYPE = "DirectoryCounter";
const id = (key: string): GrainId => new GrainId(TYPE, key);

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.dispose();
});

async function start(options: Partial<TestClusterOptions> = {}): Promise<TestCluster> {
  const cluster = await TestCluster.start({
    membership: "kubernetes",
    grains: [Counter],
    reminders: false,
    callTimeout: { seconds: 2 },
    ...options,
  });
  clusters.push(cluster);
  return cluster;
}

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

const partitionOf = (silo: TestSiloHandle): GrainDirectoryPartition =>
  (silo.host as unknown as { parts: { node: { partition: GrainDirectoryPartition } } }).parts.node
    .partition;

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

/** One call to `key` through `silo`, recorded. */
async function call(results: Results, silo: TestSiloHandle, key: string): Promise<void> {
  try {
    results.record(key, await silo.host.getGrain(Counter, key).inc());
  } catch (error) {
    results.errors.push(error);
  }
}

/**
 * Calls in flight throughout: round after round, every key is called through one of `silos()`,
 * rotating, with no wait for the cluster to settle.
 */
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

/** Keys active on more than one live silo. */
function activeTwice(cluster: TestCluster, keys: Iterable<string>): string[] {
  const out: string[] = [];
  for (const key of keys) {
    if (cluster.silos.filter((s) => s.host.isActive(id(key))).length > 1) out.push(key);
  }
  return out;
}

const keysFor = (prefix: string, n: number): string[] =>
  Array.from({ length: n }, (_, i) => `${prefix}-${i}`);

describe("the directory partition on Kubernetes membership", () => {
  it("joins a silo under load without activating any grain twice", async () => {
    const cluster = await start({ initialSilos: 2 });
    const results = new Results();
    const before = keysFor("before", 30);
    for (const [i, key] of before.entries()) await call(results, cluster.silos[i % 2]!, key);
    await cluster.checkIntegrity();

    const callers = [...cluster.silos];
    const keys = [...before];
    const load = new Load(
      results,
      () => keys,
      () => callers,
    );
    const joiner = await cluster.startAdditionalSilo({ holdReadiness: true });
    callers.push(joiner);
    // Grains first called through the joiner while it is not ready.
    const fresh = keysFor("via-joiner", 20);
    await Promise.all(fresh.map((key) => call(results, joiner, key)));
    keys.push(...fresh);
    await sleep(20);
    await cluster.checkIntegrity();

    // The instant it becomes ready, with calls in flight through every silo.
    await cluster.kubernetes.setReady(joiner, true);
    await sleep(50);
    await load.stop();
    await cluster.checkIntegrity();

    expect(results.errors).toEqual([]);
    expect(results.restarted()).toEqual([]);
    expect(activeTwice(cluster, keys)).toEqual([]);
    expect(partitionOf(joiner).view.inRing(joiner.address)).toBe(true);
  }, 60_000);

  it("survives a crash in two steps with the dead silo refusing connections", async () => {
    const cluster = await start({
      initialSilos: 3,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    const [a, b, dead] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const onSurvivors = [...keysFor("on-a", 20), ...keysFor("on-b", 20)];
    const onDead = keysFor("on-dead", 20);
    for (const key of keysFor("on-a", 20)) await call(results, a, key);
    for (const key of keysFor("on-b", 20)) await call(results, b, key);
    for (const key of onDead) await call(results, dead, key);
    await cluster.checkIntegrity();

    const recoveriesBefore = partitionOf(a).stats.recoveries + partitionOf(b).stats.recoveries;
    await cluster.killSilo(dead);
    const load = new Load(
      results,
      () => onSurvivors,
      () => [a, b],
    );
    await cluster.checkIntegrity();

    // First step: the pod turns not ready. The survivors gain its ranges and rebuild them from
    // the hosts; the dead silo refuses the connection, which is its answer.
    await cluster.kubernetes.markNotReady(dead);
    await cluster.checkIntegrity();
    expect(partitionOf(a).stats.recoveries + partitionOf(b).stats.recoveries).toBeGreaterThan(
      recoveriesBefore,
    );

    // Second step: the pod is removed.
    await cluster.kubernetes.removePod(dead);
    await load.stop();
    await cluster.checkIntegrity();

    // The survivors' grains kept their state and were never activated a second time.
    expect(results.restarted(onSurvivors)).toEqual([]);
    expect(activeTwice(cluster, onSurvivors)).toEqual([]);
    // Calls whose route needed the dead silo before it left the ring may have been refused, as
    // calls a caller may resend; nothing else failed.
    for (const error of results.errors) expect(error).toBeInstanceOf(RejectionError);
    // The dead silo's grains are placed afresh.
    for (const key of onDead) expect(await a.host.getGrain(Counter, key).inc()).toBe(1);
    await cluster.checkIntegrity();
  }, 60_000);

  it("keeps the ranges a silo gains locked while an unreachable silo is listed", async () => {
    const cluster = await start({
      initialSilos: 3,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    const [a, b, dead] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const onSurvivors = [...keysFor("on-a", 20), ...keysFor("on-b", 20)];
    for (const key of keysFor("on-a", 20)) await call(results, a, key);
    for (const key of keysFor("on-b", 20)) await call(results, b, key);
    await cluster.checkIntegrity();

    // Its process is gone, and dials to it time out instead of being refused.
    cluster.faults.failConnects({ to: dead.address });
    await cluster.killSilo(dead);
    await cluster.kubernetes.markNotReady(dead);
    const locked = [...partitionOf(a).lockedRanges, ...partitionOf(b).lockedRanges];
    expect(locked.length).toBeGreaterThan(0);

    // Calls for grains in the locked ranges wait, or are refused as calls to resend; none is
    // answered by a second activation.
    const inLocked = onSurvivors.filter((key) =>
      locked.some((r) => r.contains(id(key).getUniformHashCode())),
    );
    expect(inLocked.length).toBeGreaterThan(0);
    const waiting = inLocked.map((key) => call(results, a, key));
    await sleep(500);
    expect(partitionOf(a).lockedRanges.length + partitionOf(b).lockedRanges.length).toBeGreaterThan(
      0,
    );

    // Kubernetes removes the pod: the ranges unlock.
    await cluster.kubernetes.removePod(dead);
    await Promise.all(waiting);
    await cluster.checkIntegrity();
    expect(partitionOf(a).lockedRanges).toEqual([]);
    expect(partitionOf(b).lockedRanges).toEqual([]);
    expect(results.restarted(onSurvivors)).toEqual([]);
    expect(activeTwice(cluster, onSurvivors)).toEqual([]);
    for (const error of results.errors) expect(error).toBeInstanceOf(RejectionError);
  }, 60_000);

  it("unlocks those ranges once directoryRecoveryPatience elapses, and logs and counts it", async () => {
    const errors: Array<{ message: string; fields?: LogFields }> = [];
    const logger: Logger = {
      debug() {},
      info() {},
      warn() {},
      error: (message, fields) => void errors.push({ message, ...(fields ? { fields } : {}) }),
    };
    const cluster = await start({
      initialSilos: 3,
      logger,
      directoryRecoveryPatience: { ms: 300 },
    });
    const [a, b, dead] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    cluster.faults.failConnects({ to: dead.address });
    await cluster.killSilo(dead);
    await cluster.kubernetes.markNotReady(dead);
    expect(partitionOf(a).lockedRanges.length + partitionOf(b).lockedRanges.length).toBeGreaterThan(
      0,
    );

    await sleep(800);
    expect(partitionOf(a).lockedRanges).toEqual([]);
    expect(partitionOf(b).lockedRanges).toEqual([]);
    expect(
      partitionOf(a).stats.patienceExpired + partitionOf(b).stats.patienceExpired,
    ).toBeGreaterThan(0);
    expect(errors.some((e) => e.message.includes("recovery patience elapsed"))).toBe(true);
    // The pod is still listed, unreachable: the integrity check cannot hear from it.
    await cluster.kubernetes.removePod(dead);
    await cluster.checkIntegrity();
  }, 60_000);

  it("holds integrity through one view that adds a silo and removes another", async () => {
    const cluster = await start({
      initialSilos: 3,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    const [a, b, gone] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const keys = [...keysFor("on-a", 15), ...keysFor("on-b", 15)];
    for (const key of keysFor("on-a", 15)) await call(results, a, key);
    for (const key of keysFor("on-b", 15)) await call(results, b, key);
    const joiner = await cluster.startAdditionalSilo({ holdReadiness: true });
    await cluster.checkIntegrity();

    // Every watch is held while the joiner turns ready and the crashed pod goes; each silo then
    // re-lists and sees both changes in one view.
    await cluster.killSilo(gone);
    for (const silo of cluster.silos) cluster.kubernetes.hold(silo);
    cluster.kubernetes.api.setReady(joiner.address.podName, true);
    cluster.kubernetes.api.removePod(gone.address.podName);
    const load = new Load(
      results,
      () => keys,
      () => [a, b, joiner],
    );
    for (const silo of cluster.silos) await cluster.kubernetes.relist(silo);
    for (const silo of cluster.silos) await cluster.kubernetes.release(silo);
    await sleep(50);
    await load.stop();
    await cluster.checkIntegrity();

    expect(partitionOf(joiner).view.inRing(joiner.address)).toBe(true);
    expect(results.restarted(keys)).toEqual([]);
    expect(activeTwice(cluster, keys)).toEqual([]);
    for (const error of results.errors) expect(error).toBeInstanceOf(RejectionError);
  }, 60_000);

  it("recovers on a silo whose watch was held across two changes, instead of taking a snapshot", async () => {
    const cluster = await start({
      initialSilos: 3,
      defaultPlacementStrategy: new PreferLocalPlacement(),
    });
    const [a, b, c] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const keys = [...keysFor("on-a", 15), ...keysFor("on-b", 15), ...keysFor("on-c", 15)];
    for (const key of keysFor("on-a", 15)) await call(results, a, key);
    for (const key of keysFor("on-b", 15)) await call(results, b, key);
    for (const key of keysFor("on-c", 15)) await call(results, c, key);
    await cluster.checkIntegrity();
    const recoveriesBefore = partitionOf(a).stats.recoveries;

    cluster.kubernetes.hold(a);
    await cluster.kubernetes.markNotReady(c);
    await cluster.startAdditionalSilo();
    await cluster.kubernetes.release(a);
    const load = new Load(
      results,
      () => keys,
      () => cluster.silos,
    );
    await sleep(50);
    await load.stop();
    await cluster.checkIntegrity();

    // It moved between a pair of views no peer moved between, so no snapshot matched.
    expect(partitionOf(a).stats.recoveries).toBeGreaterThan(recoveriesBefore);
    expect(results.restarted(keys)).toEqual([]);
    expect(activeTwice(cluster, keys)).toEqual([]);
    for (const error of results.errors) expect(error).toBeInstanceOf(RejectionError);
  }, 60_000);

  it("moves a flapping silo's ranges away and back under load without activating anything twice", async () => {
    const cluster = await start({ initialSilos: 3 });
    const [, b] = cluster.silos as [TestSiloHandle, TestSiloHandle, TestSiloHandle];
    const results = new Results();
    const keys = keysFor("flap", 40);
    for (const [i, key] of keys.entries()) await call(results, cluster.silos[i % 3]!, key);
    await cluster.checkIntegrity();

    const load = new Load(
      results,
      () => keys,
      () => cluster.silos,
    );
    await cluster.kubernetes.setReady(b, false);
    await cluster.checkIntegrity();
    await cluster.kubernetes.setReady(b, true);
    await sleep(20);
    await load.stop();
    await cluster.checkIntegrity();

    expect(results.errors).toEqual([]);
    expect(results.restarted()).toEqual([]);
    expect(activeTwice(cluster, keys)).toEqual([]);
  }, 60_000);

  it("holds integrity after a cold start of three silos whose startup tasks call grains", async () => {
    const fromStartup: Record<number, number | string> = {};
    const cluster = await start({
      initialSilos: 0,
      callTimeout: { seconds: 5 },
      configureSilo: (builder, { index }) => {
        builder.addStartupTask(async (grains) => {
          fromStartup[index] = await grains
            .getGrain(Counter, `cold-${index}`)
            .inc()
            .catch((e: unknown) => `error: ${String(e)}`);
        });
      },
    });
    await cluster.startAdditionalSilos(3);
    expect(fromStartup).toEqual({ 0: 1, 1: 1, 2: 1 });
    await cluster.checkIntegrity();
    expect(activeTwice(cluster, ["cold-0", "cold-1", "cold-2"])).toEqual([]);
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

describe("directory chaos (after Orleans ElasticChaos)", () => {
  for (const seed of [11, 23, 42]) {
    it(`keeps one activation per grain through 25 rounds of start, drain and crash (seed ${seed})`, async () => {
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
            actions.push(`drain ${silo.address.podName}`);
            await cluster.stopSilo(silo);
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
      // The run did something: calls were answered throughout, and silos came and went.
      expect([...results.values.values()].flat().length).toBeGreaterThan(keys.length * 5);
      expect(new Set(actions.map((a) => a.split(" ")[0])).size).toBeGreaterThan(1);
    }, 300_000);
  }
});
