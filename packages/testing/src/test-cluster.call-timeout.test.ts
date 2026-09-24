import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { TestCluster } from "@thresh/testing/test-cluster";

interface IGated extends GrainKey<string> {
  run(): Promise<number>;
}
const IGated = defineGrainInterface<IGated>("IGated.testClusterCallTimeout");

let runs = 0;
let notifyStarted!: () => void;
let started!: Promise<void>;
let release!: () => void;
let gate!: Promise<void>;

@grain()
class GatedGrain extends Grain implements IGated {
  async run(): Promise<number> {
    runs += 1;
    notifyStarted();
    await gate;
    return runs;
  }
}

// Orleans `SiloMessagingOptions.ResponseTimeout`: how long a caller waits for
// a reply, and so also the time-to-live every request carries (issue #90).
// Configurable per cluster, as upstream stress tests bump it to a minute.
describe("TestCluster callTimeout", () => {
  it("sets how long a queued cross-silo request stays runnable", async () => {
    runs = 0;
    started = new Promise((resolve) => (notifyStarted = resolve));
    gate = new Promise((resolve) => (release = resolve));
    const time = new FakeTimeProvider();
    const cluster = await TestCluster.start({
      grains: [{ ctor: GatedGrain, interfaces: [IGated] }],
      time,
      random: () => 0.99, // place on the second silo, so calls cross the wire
      callTimeout: { minutes: 1 },
      reminders: false,
    });
    try {
      const grainRef = cluster.getGrain(IGated, "queued");
      const first = grainRef.run();
      await started;
      const second = grainRef.run();
      for (let i = 0; i < 5; i++) await new Promise((resolve) => setTimeout(resolve, 0));

      time.advance(40_000); // past the 30s default, within the configured minute
      release();

      expect(await first).toBe(1);
      expect(await second).toBe(2);
    } finally {
      await cluster.dispose();
    }
  });
});
