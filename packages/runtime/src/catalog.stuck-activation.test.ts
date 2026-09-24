import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import type { ActivationData } from "@thresh/runtime/activation";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

@grain()
class StuckGrain extends Grain {
  gate = deferred<string>();
  async block(): Promise<string> {
    return this.gate.promise;
  }
}

const metadata = getGrainMetadata(StuckGrain)!;

@grain()
class HangingDeactivateGrain extends Grain {
  static gate = deferred();
  async ping(): Promise<string> {
    return "pong";
  }
  override async onDeactivate(): Promise<void> {
    await HangingDeactivateGrain.gate.promise;
  }
}

const hangingMetadata = getGrainMetadata(HangingDeactivateGrain)!;

function buildCatalog(
  time: FakeTimeProvider,
  onDeactivated: (activation: ActivationData) => void,
): Catalog {
  const grainTypes = new Map<string, RegisteredGrain>([
    [metadata.grainType, { ctor: StuckGrain, metadata }],
  ]);
  const factory = new GrainFactory(() => metadata.grainType, time);
  return new Catalog({
    grainTypes,
    factory,
    time,
    defaultCollectionAgeSeconds: 900,
    onDeactivated,
    activationOptions: { maxRequestProcessingTimeMs: 1000 },
  });
}

describe("Catalog stuck-activation deactivation (Orleans DeactivateStuckActivation)", () => {
  it("removes a stuck activation from its map and calls onDeactivated (directory unregister) once maxRequestProcessingTimeMs is exceeded", async () => {
    const time = new FakeTimeProvider();
    const deactivated: ActivationData[] = [];
    const catalog = buildCatalog(time, (a) => deactivated.push(a));
    const id = new GrainId(metadata.grainType, "a");
    const activation = await catalog.getOrCreate(id);
    await flush();

    void activation.invoke({
      target: id,
      interfaceId: 0,
      method: "block",
      args: [],
      options: {},
      reentrancyId: "r-1",
    });
    // A waiting request blocked behind the overdue turn is what triggers
    // Orleans' stuck check.
    activation
      .invoke({
        target: id,
        interfaceId: 0,
        method: "block",
        args: [],
        options: {},
        reentrancyId: "r-1b",
      })
      .catch(() => undefined);
    await flush();
    time.advance(1000);

    expect(deactivated).toEqual([activation]);
    expect(catalog.get(id)).toBeUndefined();

    (activation.instance as StuckGrain).gate.resolve("done"); // let the wedged turn settle
  });

  it("activates a fresh activation for the next call after the stuck one is removed", async () => {
    const time = new FakeTimeProvider();
    const catalog = buildCatalog(time, () => undefined);
    const id = new GrainId(metadata.grainType, "b");
    const first = await catalog.getOrCreate(id);
    await flush();

    void first.invoke({
      target: id,
      interfaceId: 0,
      method: "block",
      args: [],
      options: {},
      reentrancyId: "r-2",
    });
    first
      .invoke({
        target: id,
        interfaceId: 0,
        method: "block",
        args: [],
        options: {},
        reentrancyId: "r-2b",
      })
      .catch(() => undefined);
    await flush();
    time.advance(1000);
    expect(catalog.get(id)).toBeUndefined();

    const second = await catalog.getOrCreate(id);
    expect(second).not.toBe(first);

    (first.instance as StuckGrain).gate.resolve("done");
  });

  it("an idle-collection sweep that was deactivating the stuck activation does not remove the fresh activation that replaced it", async () => {
    HangingDeactivateGrain.gate = deferred();
    const time = new FakeTimeProvider();
    const deactivated: ActivationData[] = [];
    const grainTypes = new Map<string, RegisteredGrain>([
      [hangingMetadata.grainType, { ctor: HangingDeactivateGrain, metadata: hangingMetadata }],
    ]);
    const catalog = new Catalog({
      grainTypes,
      factory: new GrainFactory(() => hangingMetadata.grainType, time),
      time,
      defaultCollectionAgeSeconds: 1,
      onDeactivated: (a) => deactivated.push(a),
      activationOptions: { maxRequestProcessingTimeMs: 1000 },
    });
    const id = new GrainId(hangingMetadata.grainType, "c");
    const first = await catalog.getOrCreate(id);
    await flush();
    time.advance(2000);

    const sweep = catalog.collectIdle(); // onDeactivate hangs: the blocking turn wedges
    await flush();
    first
      .invoke({
        target: id,
        interfaceId: 0,
        method: "ping",
        args: [],
        options: {},
        reentrancyId: "r-c",
      })
      .catch(() => undefined); // waits behind the wedged onDeactivate
    await flush();
    time.advance(1000); // stuck: removed from the catalog
    expect(catalog.get(id)).toBeUndefined();

    const second = await catalog.getOrCreate(id);
    await flush();
    HangingDeactivateGrain.gate.resolve(); // the orphaned onDeactivate finally completes
    await sweep;

    expect(catalog.get(id)).toBe(second);
    expect(deactivated).toEqual([first]);
  });
});
