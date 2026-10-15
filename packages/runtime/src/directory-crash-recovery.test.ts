import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import { IManagementGrain } from "@thresh/core/management-grain";
import { TestCluster } from "@thresh/testing/test-cluster";

interface ICrashCounter extends GrainKey<string> {
  increment(): Promise<number>;
}
const ICrashCounter = defineGrainInterface<ICrashCounter>("DirectoryCrashCounter");

@grain({ name: "DirectoryCrashCounter" })
class CrashCounterGrain extends Grain implements ICrashCounter {
  private count = 0;

  async increment(): Promise<number> {
    this.count += 1;
    return this.count;
  }
}

const counterId = (key: string) => new GrainId("DirectoryCrashCounter" as GrainType, key);

/**
 * A crashed silo hands nothing off, so the directory entries it owned die with it — including
 * those for grains still running on the survivors. Orleans rebuilds a crashed owner's ranges from
 * the activations every survivor reports; without that, a lookup misses, a call places a second
 * activation of a grain that never stopped, and the single-activation guarantee is gone.
 */
describe("directory recovery after a silo crash", () => {
  it("keeps each surviving grain at its one live activation, state intact", async () => {
    const cluster = await TestCluster.start({
      initialSilos: 4,
      grains: [{ ctor: CrashCounterGrain, interfaces: [ICrashCounter] }],
    });
    try {
      // Enough grains that, whatever the placement, some live on a survivor while the victim owns
      // their directory entry.
      const keys = Array.from({ length: 40 }, (_, i) => `k${i}`);
      for (const key of keys) await cluster.getGrain(ICrashCounter, key).increment();

      await cluster.killSilo(cluster.silos[3]!);

      const management = cluster.primary.host.getGrain(IManagementGrain, 0n);
      const survivors = keys.filter((key) =>
        cluster.silos.some((s) => s.host.isActive(counterId(key))),
      );
      expect(survivors.length).toBeGreaterThan(0);
      for (const key of survivors) {
        const reference = cluster.getGrain(ICrashCounter, key);
        expect(await management.getActivationAddress(reference), `${key} lost its entry`).not.toBe(
          null,
        );
        expect(await reference.increment(), `${key} was activated a second time`).toBe(2);
        const hosts = cluster.silos.filter((s) => s.host.isActive(counterId(key)));
        expect(hosts, `${key} has more than one activation`).toHaveLength(1);
      }
    } finally {
      await cluster.dispose();
    }
  });
});
