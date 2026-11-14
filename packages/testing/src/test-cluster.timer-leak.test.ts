import { afterEach, describe, expect, it } from "vitest";
import { grain, implicitStreamSubscription } from "@thresh/core/decorators";
import {
  completed,
  DURABLE_JOB_HANDLER,
  type DurableJobReceiver,
  type DurableJobRunResult,
} from "@thresh/core/durable-job";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { Remindable } from "@thresh/core/reminder";
import { STREAM_SUBSCRIPTION_OBSERVER, type StreamHandler } from "@thresh/core/stream";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { PendingTimerTracker } from "@thresh/core/test-support/pending-timer-tracker";
import { TestCluster, type TestClusterOptions } from "@thresh/testing/test-cluster";

interface ILeakProbe extends GrainKey<string> {
  touch(): Promise<number>;
  remind(): Promise<void>;
  scheduleJob(): Promise<void>;
}
const ILeakProbe = defineGrainInterface<ILeakProbe>("ILeakProbe.testClusterTimerLeak");

@grain({ name: "TestClusterTimerLeakProbe" })
@implicitStreamSubscription("leak-probe")
class LeakProbeGrain extends Grain implements ILeakProbe, Remindable, DurableJobReceiver {
  private touches = 0;

  async touch(): Promise<number> {
    this.touches += 1;
    return this.touches;
  }

  async remind(): Promise<void> {
    await this.runtime.registerReminder("beat", { seconds: 30 }, { minutes: 1 });
  }

  async scheduleJob(): Promise<void> {
    await this.runtime.scheduleJob({
      target: this.id,
      name: "later",
      dueTime: new Date(this.runtime.timeProvider.now() + 20_000),
    });
  }

  async receiveReminder(): Promise<void> {}

  async [DURABLE_JOB_HANDLER](): Promise<DurableJobRunResult> {
    return completed;
  }

  [STREAM_SUBSCRIPTION_OBSERVER](): StreamHandler<string> {
    return { onNext: async () => {} };
  }
}

const grains = [{ ctor: LeakProbeGrain, interfaces: [ILeakProbe] }];

let clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters) await c.dispose();
  clusters = [];
});

async function startTracked(
  options: Partial<TestClusterOptions> = {},
): Promise<{ cluster: TestCluster; time: PendingTimerTracker }> {
  const time = new PendingTimerTracker(new FakeTimeProvider());
  const cluster = await TestCluster.start({ initialSilos: 2, grains, time, ...options });
  clusters.push(cluster);
  return { cluster, time };
}

/** Let any work still settling after dispose (a reconcile, a released claim) run to its end. */
async function settle(): Promise<void> {
  for (let i = 0; i < 20; i += 1) await new Promise((resolve) => setImmediate(resolve));
}

async function expectNoTimerAfterDispose(
  cluster: TestCluster,
  time: PendingTimerTracker,
): Promise<void> {
  await cluster.dispose();
  await settle();
  expect(time.pendingTimers().map((t) => `${t.delayMs}ms\n${t.armedAt}`)).toEqual([]);
}

describe("TestCluster leaves no timer armed once disposed", () => {
  it("a plain cluster", async () => {
    const { cluster, time } = await startTracked();
    await cluster.getGrain(ILeakProbe, "a").touch();
    await expectNoTimerAfterDispose(cluster, time);
  });

  it("a cluster with reminders registered", async () => {
    const { cluster, time } = await startTracked();
    for (const key of ["a", "b", "c", "d"]) await cluster.getGrain(ILeakProbe, key).remind();
    await expectNoTimerAfterDispose(cluster, time);
  });

  it("a cluster with memory durable jobs scheduled", async () => {
    const { cluster, time } = await startTracked();
    for (const key of ["a", "b", "c", "d"]) await cluster.getGrain(ILeakProbe, key).scheduleJob();
    await expectNoTimerAfterDispose(cluster, time);
  });

  it("a cluster with memory streams in use", async () => {
    const { cluster, time } = await startTracked();
    for (const key of ["a", "b", "c", "d"]) {
      await cluster.streamProvider().getStream<string>("leak-probe", key).publish("hello");
    }
    await expectNoTimerAfterDispose(cluster, time);
  });

  it("a cluster on Kubernetes membership", async () => {
    const { cluster, time } = await startTracked({ membership: "kubernetes" });
    for (const key of ["a", "b", "c", "d"]) await cluster.getGrain(ILeakProbe, key).remind();
    await expectNoTimerAfterDispose(cluster, time);
  });

  it("a cluster whose view changes while a silo is stopping", async () => {
    const { cluster, time } = await startTracked();
    for (const key of ["a", "b", "c", "d"]) await cluster.getGrain(ILeakProbe, key).remind();
    // The new silo's arrival publishes a view to every silo, the stopping one included.
    await Promise.all([cluster.stopSilo(cluster.silos[0]!), cluster.startAdditionalSilo()]);
    await expectNoTimerAfterDispose(cluster, time);
  });

  it("a Kubernetes cluster whose view changes while a silo is stopping", async () => {
    const { cluster, time } = await startTracked({ membership: "kubernetes" });
    for (const key of ["a", "b", "c", "d"]) await cluster.getGrain(ILeakProbe, key).remind();
    const survivor = cluster.silos[1]!;
    await Promise.all([
      cluster.stopSilo(cluster.silos[0]!),
      cluster.kubernetes
        .setReady(survivor, false)
        .then(() => cluster.kubernetes.setReady(survivor, true)),
    ]);
    await expectNoTimerAfterDispose(cluster, time);
  });
});
