import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// L7: an idle sweep selects the stale activations in one pass and
// deactivates them with bounded concurrency above one (Orleans
// `DeactivateActivationsFromCollector`: `Parallel.ForEachAsync`), so its
// throughput is not one deactivation per hook latency.

const HOOK_MS = 100;
const clock = { time: new FakeTimeProvider() };

@grain()
class SlowHookGrain extends Grain {
  override async onDeactivate(): Promise<void> {
    await new Promise<void>((resolve) => clock.time.setTimer(resolve, HOOK_MS));
  }
}

const metadata = getGrainMetadata(SlowHookGrain)!;

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("Catalog.collectIdle concurrency (L7)", () => {
  it("finishes a sweep over many stale activations in fewer than that-many hook times", async () => {
    const time = new FakeTimeProvider();
    clock.time = time;
    const catalog = new Catalog({
      grainTypes: new Map<string, RegisteredGrain>([
        [metadata.grainType, { ctor: SlowHookGrain, metadata }],
      ]),
      factory: new GrainFactory(() => metadata.grainType, time),
      time,
      defaultCollectionAgeSeconds: 900,
    });
    const count = 8;
    for (let i = 0; i < count; i++) {
      await catalog.getOrCreate(new GrainId(metadata.grainType, `g${i}`));
    }
    await flush();
    expect(catalog.count()).toBe(count);

    let done = false;
    const sweep = catalog.collectIdle(0).then(() => (done = true));
    await flush();

    // One hook time: a sequential sweep would have finished one activation.
    time.advance(HOOK_MS);
    await flush();
    await flush();

    expect(done).toBe(true);
    expect(catalog.count()).toBe(0);
    await sweep;
  });
});
