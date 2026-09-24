import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { RejectionError } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

@grain()
class StoppingCatalogGrain extends Grain {}

const metadata = getGrainMetadata(StoppingCatalogGrain)!;

function buildCatalog(): Catalog {
  const grainTypes = new Map<string, RegisteredGrain>([
    [metadata.grainType, { ctor: StoppingCatalogGrain, metadata }],
  ]);
  const time = new FakeTimeProvider();
  const factory = new GrainFactory(() => metadata.grainType, time);
  return new Catalog({ grainTypes, factory, time, defaultCollectionAgeSeconds: 900 });
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("Catalog refuses to create activations once deactivateAll has started (issue #108)", () => {
  it("rejects a getOrCreate for a grain id deactivateAll's snapshot never saw", async () => {
    const catalog = buildCatalog();
    const idA = new GrainId(metadata.grainType, "a");
    const idB = new GrainId(metadata.grainType, "b");
    await catalog.getOrCreate(idA);
    await flush();

    const deactivating = catalog.deactivateAll({
      code: "shutting-down",
      description: "silo stopping",
    });

    // A `getOrCreate` for a DIFFERENT id, racing the sweep, must not create
    // an orphan that deactivateAll's already-taken snapshot will never
    // deactivate or unregister.
    await expect(catalog.getOrCreate(idB)).rejects.toThrow(RejectionError);
    await expect(catalog.getOrCreate(idB)).rejects.toMatchObject({ kind: "siloDraining" });

    await deactivating;

    expect(catalog.countFor(idB)).toBe(0);
    expect(catalog.count()).toBe(0);
  });
});
