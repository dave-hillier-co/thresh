import { afterEach, describe, expect, it } from "vitest";
import { grain, persistentState } from "@thresh/core/decorators";
import {
  completed,
  DURABLE_JOB_HANDLER,
  type DurableJobReceiver,
  type DurableJobRunResult,
} from "@thresh/core/durable-job";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
import type { Remindable } from "@thresh/core/reminder";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

/** Every piece of background work, with the silo it ran on. */
let events: Array<{ silo: string; kind: string }> = [];

interface IWorker extends GrainKey<string> {
  start(): Promise<void>;
  ping(): Promise<string>;
}
const IWorker = defineGrainInterface<IWorker>("IWorker.testClusterKill");

/**
 * A grain that keeps a timer, a reminder and a durable job going, each writing its state to
 * the cluster's shared store, and records its deactivation hook.
 */
@grain({ name: "TestClusterKillWorker" })
class WorkerGrain extends Grain implements IWorker, Remindable, DurableJobReceiver {
  @persistentState("work", { defaultValue: () => ({ runs: 0, lastSilo: "" }) })
  private work!: PersistentState<{ runs: number; lastSilo: string }>;

  async start(): Promise<void> {
    this.runtime.registerTimer(() => this.record("timer"), { seconds: 1 }, { seconds: 1 });
    await this.runtime.registerReminder("beat", { seconds: 30 }, { minutes: 1 });
    await this.runtime.scheduleJob({
      target: this.id,
      name: "later",
      dueTime: new Date(this.runtime.timeProvider.now() + 20_000),
    });
  }

  async ping(): Promise<string> {
    events.push({ silo: this.silo(), kind: "ping" });
    return this.silo();
  }

  async receiveReminder(): Promise<void> {
    await this.record("reminder");
  }

  async [DURABLE_JOB_HANDLER](): Promise<DurableJobRunResult> {
    await this.record("job");
    return completed;
  }

  override async onDeactivate(): Promise<void> {
    events.push({ silo: this.silo(), kind: "deactivate" });
  }

  private silo(): string {
    return this.runtime.localSiloAddress().podName;
  }

  private async record(kind: string): Promise<void> {
    events.push({ silo: this.silo(), kind });
    this.work.value.runs += 1;
    this.work.value.lastSilo = this.silo();
    await this.work.write();
  }
}

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.dispose();
});

async function start(time: FakeTimeProvider): Promise<TestCluster> {
  events = [];
  const cluster = await TestCluster.start({
    grains: [{ ctor: WorkerGrain, interfaces: [IWorker] }],
    time,
    defaultPlacementStrategy: new PreferLocalPlacement(),
    callTimeout: { seconds: 2 },
  });
  clusters.push(cluster);
  return cluster;
}

const flush = async (): Promise<void> => {
  for (let i = 0; i < 5; i++) await new Promise((resolve) => setTimeout(resolve, 0));
};

async function runFor(time: FakeTimeProvider, seconds: number): Promise<void> {
  for (let i = 0; i < seconds; i++) {
    time.advance(1000);
    await flush();
  }
}

describe("TestCluster.killSilo", () => {
  it("stops the killed silo's timers, reminders and jobs, and runs no deactivation hook", async () => {
    const time = new FakeTimeProvider();
    const cluster = await start(time);
    const [a, b] = cluster.silos as [TestSiloHandle, TestSiloHandle];
    await b.host.getGrain(IWorker, "w").start();
    await runFor(time, 3);
    expect(events.filter((e) => e.silo === b.address.podName && e.kind === "timer").length).toBe(3);

    await cluster.killSilo(b);
    const killedAt = events.length;
    await runFor(time, 240);

    const afterKill = events.slice(killedAt);
    expect(afterKill.filter((e) => e.silo === b.address.podName)).toEqual([]);
    expect(events.filter((e) => e.kind === "deactivate")).toEqual([]);
    // The survivor takes the reminder over, as it would from a crashed pod.
    expect(afterKill.some((e) => e.silo === a.address.podName && e.kind === "reminder")).toBe(true);
  });

  it("leaves the killed silo unable to send", async () => {
    const time = new FakeTimeProvider();
    const cluster = await start(time);
    const [a, b] = cluster.silos as [TestSiloHandle, TestSiloHandle];
    expect(await a.host.getGrain(IWorker, "on-a").ping()).toBe(a.address.podName);
    expect(await b.host.getGrain(IWorker, "on-a").ping()).toBe(a.address.podName);

    await cluster.killSilo(b);
    const killedAt = events.length;
    void b.host
      .getGrain(IWorker, "on-a")
      .ping()
      .catch(() => undefined);
    await runFor(time, 5);
    // Nothing the dead silo tried to send reached the survivor.
    expect(events.slice(killedAt)).toEqual([]);
  });

  it("still drains a gracefully stopped silo: its deactivation hooks run", async () => {
    const time = new FakeTimeProvider();
    const cluster = await start(time);
    const [, b] = cluster.silos as [TestSiloHandle, TestSiloHandle];
    await b.host.getGrain(IWorker, "w").start();
    await runFor(time, 1);

    await cluster.stopSilo(b);
    expect(events).toContainEqual({ silo: b.address.podName, kind: "deactivate" });
  });
});
