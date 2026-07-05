import { describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainType } from "@tsva/core/grain-type";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { TestCluster } from "@tsva/testing/test-cluster";
import { waitFor } from "@tsva/testing/wait";

interface ICounter extends GrainWithStringKey {
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
});
