import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

/** `onDeactivate` never resolves on its own — only this test's fake clock can end it. */
@grain()
class HangingDeactivateGrain extends Grain {
  override async onDeactivate(): Promise<void> {
    await new Promise<void>(() => {
      // deliberately never resolves
    });
  }
}

const metadata = getGrainMetadata(HangingDeactivateGrain)!;

function buildCatalog(
  time: FakeTimeProvider,
  onDeactivated: (activation: unknown) => void,
): Catalog {
  const grainTypes = new Map<string, RegisteredGrain>([
    [metadata.grainType, { ctor: HangingDeactivateGrain, metadata }],
  ]);
  const factory = new GrainFactory(() => metadata.grainType, time);
  return new Catalog({
    grainTypes,
    factory,
    time,
    defaultCollectionAgeSeconds: 900,
    onDeactivated,
  });
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe("Catalog.deactivateAll's overall deadline (issue #108)", () => {
  it("stops waiting once the deadline passes, leaving a still-hung activation alone", async () => {
    const time = new FakeTimeProvider();
    const deactivated: unknown[] = [];
    const catalog = buildCatalog(time, (a) => deactivated.push(a));
    const id = new GrainId(metadata.grainType, "a");
    const activation = await catalog.getOrCreate(id);
    await flush(); // let onActivate finish so the activation is "valid"

    const deactivating = catalog.deactivateAll(
      { code: "shutting-down", description: "silo stopping" },
      5000,
    );

    // The hook never resolves on its own; only the overall deadline can end
    // this sweep. Resolve once the deadline is reached — proves the sweep
    // does not hang forever waiting on it.
    time.advance(5000);
    await deactivating;

    // The hung activation is left mid-deactivation, not disposed/unregistered
    // out from under its still-running hook.
    expect(activation.state).toBe("deactivating");
    expect(deactivated).toHaveLength(0);
  });

  it("waits for every activation when no deadline is given (existing behaviour)", async () => {
    const time = new FakeTimeProvider();
    const deactivated: unknown[] = [];
    const catalog = buildCatalog(time, (a) => deactivated.push(a));
    const id = new GrainId(metadata.grainType, "a");
    await catalog.getOrCreate(id);
    await flush();

    // No deadline: would hang forever on the real hook, so race it against a
    // short timeout to prove *this test* fails loudly rather than hanging,
    // while asserting the sweep is still pending (not falsely resolved).
    const deactivating = catalog.deactivateAll({
      code: "shutting-down",
      description: "silo stopping",
    });
    const settled = await Promise.race([
      deactivating.then(() => "settled" as const),
      new Promise<"pending">((resolve) => setTimeout(() => resolve("pending"), 20)),
    ]);
    expect(settled).toBe("pending");
  });
});
