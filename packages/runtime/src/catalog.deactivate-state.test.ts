import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

@grain()
class DeactivateStateGrain extends Grain {}

const metadata = getGrainMetadata(DeactivateStateGrain)!;

function buildCatalog(deactivateState: (instance: Grain, grainId: GrainId) => void): Catalog {
  const grainTypes = new Map<string, RegisteredGrain>([
    [metadata.grainType, { ctor: DeactivateStateGrain, metadata }],
  ]);
  const time = new FakeTimeProvider();
  const factory = new GrainFactory(() => metadata.grainType, time);
  return new Catalog({
    grainTypes,
    factory,
    time,
    defaultCollectionAgeSeconds: 900,
    deactivateState,
  });
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("Catalog deactivateState hook", () => {
  it("is called with the instance and grain id when deactivateAll runs (silo shutdown)", async () => {
    const calls: Array<{ instance: Grain; grainId: GrainId }> = [];
    const catalog = buildCatalog((instance, grainId) => calls.push({ instance, grainId }));
    const id = new GrainId(metadata.grainType, "a");
    const activation = await catalog.getOrCreate(id);

    await catalog.deactivateAll({ code: "shutting-down", description: "silo stopping" });

    expect(calls).toHaveLength(1);
    expect(calls[0]!.instance).toBe(activation.instance);
    expect(calls[0]!.grainId.toString()).toBe(id.toString());
  });

  it("is called once per activation when idle collection deactivates it", async () => {
    const calls: Array<{ instance: Grain; grainId: GrainId }> = [];
    const catalog = buildCatalog((instance, grainId) => calls.push({ instance, grainId }));
    const id = new GrainId(metadata.grainType, "b");
    const activation = await catalog.getOrCreate(id);
    await flush(); // let the scheduled activation turn (onActivate) finish

    // Force-collect regardless of age (IManagementGrain.forceActivationCollection's path).
    await catalog.collectIdle(0);

    expect(calls).toHaveLength(1);
    expect(calls[0]!.instance).toBe(activation.instance);
  });

  it("does not fire deactivateState while an activation stays live", async () => {
    const calls: Array<{ instance: Grain; grainId: GrainId }> = [];
    const catalog = buildCatalog((instance, grainId) => calls.push({ instance, grainId }));
    const id = new GrainId(metadata.grainType, "c");
    await catalog.getOrCreate(id);

    expect(calls).toHaveLength(0);
  });
});
