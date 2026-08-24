import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { defineGrain } from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";

interface ICounter extends GrainKey<string> {
  increment(): Promise<number>;
}
const ICounter = defineGrainInterface<ICounter>("TestClusterCounter");

@grain({ name: "TestClusterCounter" })
class CounterGrain extends Grain implements ICounter {
  private count = 0;

  async increment(): Promise<number> {
    this.count += 1;
    return this.count;
  }
}

const counterId = (key: string) => new GrainId("TestClusterCounter" as GrainType, key);

// Regression coverage for the teardown race (#27): `onDeactivate` making a
// cross-silo call must still land while the peer's transport is up, whether
// the deactivation is driven by a single `stopSilo()` or by `dispose()`
// tearing down every silo. `notifications` lives outside grain state so it
// survives the watcher's own silo being stopped.
let notifications: string[] = [];
let watcherTargetKey = "watcher";

interface IWatcherGrain extends GrainKey<string> {
  ping(): Promise<void>;
  notify(fromKey: string): Promise<void>;
}
const IWatcherGrain = defineGrainInterface<IWatcherGrain>("TeardownRaceWatcher");

@grain({ name: "TeardownRaceWatcher" })
class WatcherGrain extends Grain implements IWatcherGrain {
  async ping(): Promise<void> {
    // Activation-only call, so a test can place the watcher before wiring notifiers to it.
  }

  async notify(fromKey: string): Promise<void> {
    notifications.push(fromKey);
  }
}

interface INotifierGrain extends GrainKey<string> {
  touch(): Promise<void>;
}
const INotifierGrain = defineGrainInterface<INotifierGrain>("TeardownRaceNotifier");

@grain({ name: "TeardownRaceNotifier" })
class NotifierGrain extends Grain implements INotifierGrain {
  async touch(): Promise<void> {
    // Activate; the interesting behaviour is entirely in onDeactivate below.
  }

  override async onDeactivate(): Promise<void> {
    const watcher = this.getGrain(IWatcherGrain, watcherTargetKey);
    await watcher.notify(String(this.id.key));
  }
}

/** A fused definition: it carries its own interface, so `grains` takes it as-is. */
const Tally = defineGrain("TestClusterTally", () => {
  let total = 0;
  return {
    add: async (n: number) => {
      total += n;
      return total;
    },
  };
});

interface ITally {
  add(n: number): Promise<number>;
}
const ISeparateTally = defineGrainInterface<ITally>("TestClusterSeparateTally");

const notifierId = (key: string) => new GrainId("TeardownRaceNotifier" as GrainType, key);
const watcherId = (key: string) => new GrainId("TeardownRaceWatcher" as GrainType, key);

describe("TestCluster", () => {
  it("starts N silos and serves a grain with one activation across the cluster", async () => {
    const cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: CounterGrain, interfaces: [ICounter] }],
    });
    try {
      expect(cluster.silos).toHaveLength(2);

      // Calls from every silo reach the same single activation.
      expect(await cluster.silos[0]!.host.getGrain(ICounter, "c1").increment()).toBe(1);
      expect(await cluster.silos[1]!.host.getGrain(ICounter, "c1").increment()).toBe(2);
      expect(await cluster.getGrain(ICounter, "c1").increment()).toBe(3);

      const hosts = cluster.silos.filter((s) => s.host.isActive(counterId("c1")));
      expect(hosts).toHaveLength(1);
    } finally {
      await cluster.dispose();
    }
  });

  it("startAdditionalSilo joins a silo that can serve calls", async () => {
    const cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: CounterGrain, interfaces: [ICounter] }],
    });
    try {
      const added = await cluster.startAdditionalSilo();
      expect(cluster.silos).toHaveLength(2);
      expect(await added.host.getGrain(ICounter, "c2").increment()).toBe(1);
    } finally {
      await cluster.dispose();
    }
  });

  it("killSilo abruptly removes a silo and the grain reactivates on a survivor", async () => {
    const cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: CounterGrain, interfaces: [ICounter] }],
    });
    try {
      // Touch enough grains that at least one lands on each silo.
      const keys = Array.from({ length: 8 }, (_, i) => `k${i}`);
      for (const key of keys) await cluster.getGrain(ICounter, key).increment();
      const victim = cluster.silos[1]!;
      const victimKeys = keys.filter((key) => victim.host.isActive(counterId(key)));
      expect(victimKeys.length).toBeGreaterThan(0);

      await cluster.killSilo(victim);
      expect(cluster.silos).toHaveLength(1);

      // A killed silo's grains reactivate (fresh state) on a survivor on next call.
      const key = victimKeys[0]!;
      await waitFor(async () => (await cluster.getGrain(ICounter, key).increment()) === 1);
      expect(cluster.silos[0]!.host.isActive(counterId(key))).toBe(true);
    } finally {
      await cluster.dispose();
    }
  });

  it("stopSilo gracefully removes a silo from the cluster", async () => {
    const cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: CounterGrain, interfaces: [ICounter] }],
    });
    try {
      await cluster.stopSilo(cluster.silos[1]!);
      expect(cluster.silos).toHaveLength(1);
      expect(await cluster.getGrain(ICounter, "after-stop").increment()).toBe(1);
    } finally {
      await cluster.dispose();
    }
  });

  it("stopSilo lets onDeactivate finish a cross-silo call before the transport closes", async () => {
    notifications = [];
    const cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: WatcherGrain, interfaces: [IWatcherGrain] },
        { ctor: NotifierGrain, interfaces: [INotifierGrain] },
      ],
    });
    try {
      watcherTargetKey = "watcher";
      await cluster.getGrain(IWatcherGrain, "watcher").ping();
      const watcherSilo = cluster.silos.find((s) => s.host.isActive(watcherId("watcher")))!;
      const otherSilo = cluster.silos.find((s) => s !== watcherSilo)!;

      // Land the notifier on the silo that does *not* host the watcher, so its
      // onDeactivate must reach across silos.
      const keys = Array.from({ length: 8 }, (_, i) => `stop-${i}`);
      for (const key of keys) await cluster.getGrain(INotifierGrain, key).touch();
      const crossSiloKey = keys.find((key) => otherSilo.host.isActive(notifierId(key)));
      expect(crossSiloKey).toBeDefined();

      await cluster.stopSilo(otherSilo);

      expect(notifications).toContain(crossSiloKey);
    } finally {
      await cluster.dispose();
    }
  });

  it("dispose() stops silos in order, so an earlier silo's onDeactivate can still reach a later one", async () => {
    notifications = [];
    const cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: WatcherGrain, interfaces: [IWatcherGrain] },
        { ctor: NotifierGrain, interfaces: [INotifierGrain] },
      ],
    });

    // dispose() stops `cluster.silos` in order, so a notifier hosted on
    // `silos[0]` deactivates while `silos[1]` (hosting the watcher) is still
    // up; a notifier hosted on `silos[1]` would race its own watcher's
    // teardown, which is a separate, pre-existing hazard this issue doesn't
    // cover — so pin the watcher onto the later silo and only assert on
    // notifiers hosted on the earlier one.
    const lastSilo = cluster.silos[cluster.silos.length - 1]!;
    let watcherKey: string | undefined;
    for (let i = 0; i < 20 && watcherKey === undefined; i += 1) {
      const candidate = `watcher-${i}`;
      await cluster.getGrain(IWatcherGrain, candidate).ping();
      if (lastSilo.host.isActive(watcherId(candidate))) {
        watcherKey = candidate;
      }
    }
    expect(watcherKey).toBeDefined();
    watcherTargetKey = watcherKey!;

    const earlySilo = cluster.silos.find((s) => s !== lastSilo)!;
    const keys = Array.from({ length: 12 }, (_, i) => `dispose-${i}`);
    for (const key of keys) await cluster.getGrain(INotifierGrain, key).touch();
    const crossSiloKeys = keys.filter((key) => earlySilo.host.isActive(notifierId(key)));
    expect(crossSiloKeys.length).toBeGreaterThan(0);

    await cluster.dispose();

    for (const key of crossSiloKeys) {
      expect(notifications).toContain(key);
    }
  });
});

describe("TestCluster grain registration", () => {
  it("accepts a bare definition, registering it under its own interface", async () => {
    const cluster = await TestCluster.start({ initialSilos: 2, grains: [Tally] });
    try {
      expect(await cluster.getGrain(Tally, "t1").add(2)).toBe(2);
      expect(await cluster.silos[1]!.host.getGrain(Tally, "t1").add(3)).toBe(5);
    } finally {
      await cluster.dispose();
    }
  });

  it("registers a definition under exactly the interfaces it is given", async () => {
    const cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: Tally.grain, interfaces: [ISeparateTally] }],
    });
    try {
      expect(await cluster.getGrain(ISeparateTally, "t2").add(4)).toBe(4);
    } finally {
      await cluster.dispose();
    }
  });

  it("mixes bare definitions with constructor specs", async () => {
    const cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [Tally, { ctor: CounterGrain, interfaces: [ICounter] }],
    });
    try {
      expect(await cluster.getGrain(Tally, "t3").add(1)).toBe(1);
      expect(await cluster.getGrain(ICounter, "t3").increment()).toBe(1);
    } finally {
      await cluster.dispose();
    }
  });
});
