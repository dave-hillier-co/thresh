import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

/**
 * `onDeactivate` runs a test-supplied hook, letting a test interleave a
 * `getOrCreate` in the exact spot `collectOne` is awaiting it — the race
 * from issue #106 (a fresh reactivation lands while the old activation's
 * hook is still in flight).
 */
const raceHook: { current: (() => Promise<void>) | undefined } = { current: undefined };

@grain()
class RacyGrain extends Grain {
  override async onDeactivate(): Promise<void> {
    const hook = raceHook.current;
    raceHook.current = undefined;
    await hook?.();
  }
}

const metadata = getGrainMetadata(RacyGrain)!;

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

function buildCatalog(): Catalog {
  const grainTypes = new Map<string, RegisteredGrain>([
    [metadata.grainType, { ctor: RacyGrain, metadata }],
  ]);
  const time = new FakeTimeProvider();
  const factory = new GrainFactory(() => metadata.grainType, time);
  return new Catalog({
    grainTypes,
    factory,
    time,
    defaultCollectionAgeSeconds: 900,
  });
}

describe("Catalog.collectIdle racing a reactivation", () => {
  it("a lookup while the hook runs neither finalizes the old activation nor loses the next one", async () => {
    const catalog = buildCatalog();
    const id = new GrainId(metadata.grainType, "a");
    const a = await catalog.getOrCreate(id);
    await flush();

    // While `collectOne` is awaiting `a`'s deactivate hook, another caller
    // looks the grain up (issue #106's interleaving). Deactivation has one
    // owner (L1): the lookup gets `a` itself, still deactivating, whose calls
    // are held and rerouted once it has finished — nobody finalizes `a` under
    // its running hook, and nothing is left behind for the sweep to delete.
    let seenDuringHook: Awaited<ReturnType<Catalog["getOrCreate"]>> | undefined;
    raceHook.current = async () => {
      seenDuringHook = await catalog.getOrCreate(id);
    };

    // ageLimitOverrideMs = 0 forces collection of `a` regardless of age.
    await catalog.collectIdle(0);

    expect(seenDuringHook).toBe(a);
    expect(a.state).toBe("invalid");
    expect(catalog.get(id)).toBeUndefined();
    const b = await catalog.getOrCreate(id);
    await flush();
    expect(b).not.toBe(a);
    expect(catalog.countFor(id)).toBe(1);
    expect(b.state).toBe("valid");
  });
});
