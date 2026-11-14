import { describe, expect, it } from "vitest";
import { grain } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { getGrainMetadata } from "@thresh/core/grain-metadata";
import type { InvocationRequest } from "@thresh/core/request";
import type { ActivationData } from "@thresh/runtime/activation";
import { Catalog, type RegisteredGrain } from "@thresh/runtime/catalog";
import { GrainFactory } from "@thresh/runtime/grain-factory";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

// L1: deactivation has ONE owner — the activation's own pipeline (Orleans
// `ActivationData.Deactivate` + `FinishDeactivating`): every caller joins one
// completion, the hook runs once, and the catalog's parts (removal, disposal,
// state unbinding, directory unregister) run exactly once, with no invalid
// entry left behind for a sweep.

function deferred<T = void>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((res) => {
    resolve = res;
  });
  return { promise, resolve };
}

const flush = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

const probe = {
  hookStarted: 0,
  hookFinished: 0,
  writes: 0,
  gate: undefined as { promise: Promise<void>; resolve: () => void } | undefined,
};

function resetProbe(): void {
  probe.hookStarted = 0;
  probe.hookFinished = 0;
  probe.writes = 0;
  probe.gate = undefined;
}

@grain()
class PipelineGrain extends Grain {
  async ping(): Promise<string> {
    return "pong";
  }
  override async onDeactivate(): Promise<void> {
    probe.hookStarted++;
    await probe.gate?.promise;
    // A state write made inside the hook (a storage round-trip).
    await Promise.resolve();
    probe.writes++;
    probe.hookFinished++;
  }
}

@grain()
class FailingActivationGrain extends Grain {
  override async onActivate(): Promise<void> {
    throw new Error("activation boom");
  }
  async ping(): Promise<string> {
    return "pong";
  }
}

@grain()
class SelfDeactivatingGrain extends Grain {
  override async onActivate(): Promise<void> {
    this.runtime.deactivateOnIdle();
  }
  async ping(): Promise<string> {
    return "pong";
  }
}

const pipelineMeta = getGrainMetadata(PipelineGrain)!;
const failingMeta = getGrainMetadata(FailingActivationGrain)!;
const selfMeta = getGrainMetadata(SelfDeactivatingGrain)!;

interface Counters {
  disposed: Grain[];
  unbound: Grain[];
  unregistered: ActivationData[];
  /** How many hooks were still running when each finalization step ran. */
  hookRunningAtFinalize: number[];
}

function buildCatalog(time = new FakeTimeProvider()): { catalog: Catalog; counters: Counters } {
  const counters: Counters = {
    disposed: [],
    unbound: [],
    unregistered: [],
    hookRunningAtFinalize: [],
  };
  const grainTypes = new Map<string, RegisteredGrain>([
    [pipelineMeta.grainType, { ctor: PipelineGrain, metadata: pipelineMeta }],
    [failingMeta.grainType, { ctor: FailingActivationGrain, metadata: failingMeta }],
    [selfMeta.grainType, { ctor: SelfDeactivatingGrain, metadata: selfMeta }],
  ]);
  const catalog = new Catalog({
    grainTypes,
    factory: new GrainFactory(() => pipelineMeta.grainType, time),
    time,
    defaultCollectionAgeSeconds: 900,
    grainActivator: {
      createInstance: (ctor) => new (ctor as unknown as new () => Grain)(),
      disposeInstance: (instance) => {
        counters.disposed.push(instance);
      },
    },
    deactivateState: (instance) => {
      counters.hookRunningAtFinalize.push(probe.hookStarted - probe.hookFinished);
      counters.unbound.push(instance);
    },
    onDeactivated: (a) => {
      counters.hookRunningAtFinalize.push(probe.hookStarted - probe.hookFinished);
      counters.unregistered.push(a);
    },
  });
  return { catalog, counters };
}

const ping = (id: GrainId, reentrancyId: string): InvocationRequest => ({
  target: id,
  interfaceId: 0,
  method: "ping",
  args: [],
  options: {},
  reentrancyId,
});

describe("one deactivation pipeline, owned by the activation (L1)", () => {
  it("joins a second concurrent deactivation: the hook and finalization run once, and both callers resolve only after it finished", async () => {
    resetProbe();
    const { catalog, counters } = buildCatalog();
    const id = new GrainId(pipelineMeta.grainType, "a");
    const activation = await catalog.getOrCreate(id);
    await flush();
    probe.gate = deferred();

    const settled: string[] = [];
    const first = activation
      .deactivate({ code: "application-requested", description: "first" })
      .then(() => settled.push("first"));
    const second = activation
      .deactivate({ code: "application-requested", description: "second" })
      .then(() => settled.push("second"));
    await flush();

    expect(settled).toEqual([]);
    expect(probe.hookStarted).toBe(1);

    probe.gate.resolve();
    await Promise.all([first, second]);

    expect(probe.hookStarted).toBe(1);
    expect(probe.hookFinished).toBe(1);
    expect(counters.disposed).toEqual([activation.instance]);
    expect(counters.unbound).toEqual([activation.instance]);
    expect(counters.unregistered).toEqual([activation]);
    expect(catalog.get(id)).toBeUndefined();
  });

  it("a silo stop arriving mid-sweep does not finalize or unregister the activation under its running hook, and the hook's write completes", async () => {
    resetProbe();
    const { catalog, counters } = buildCatalog();
    const id = new GrainId(pipelineMeta.grainType, "b");
    await catalog.getOrCreate(id);
    await flush();
    probe.gate = deferred();

    const sweep = catalog.collectIdle(0);
    await flush();
    expect(probe.hookStarted).toBe(1);

    let stopped = false;
    const stopping = catalog
      .deactivateAll({ code: "shutting-down", description: "node stopping" })
      .then(() => (stopped = true));
    await flush();

    expect(stopped).toBe(false);
    expect(counters.unregistered).toEqual([]);
    expect(counters.unbound).toEqual([]);

    probe.gate.resolve();
    await Promise.all([sweep, stopping]);

    expect(probe.writes).toBe(1);
    expect(probe.hookStarted).toBe(1);
    expect(counters.unregistered).toHaveLength(1);
    expect(counters.unbound).toHaveLength(1);
    expect(counters.disposed).toHaveLength(1);
    // Every finalization step ran after the hook had returned.
    expect(counters.hookRunningAtFinalize.every((n) => n === 0)).toBe(true);
  });

  it("a direct deactivate (a migration that lost the directory CAS) leaves no invalid entry and disposes and unbinds once", async () => {
    resetProbe();
    const { catalog, counters } = buildCatalog();
    const id = new GrainId(pipelineMeta.grainType, "c");
    const activation = await catalog.getOrCreate(id);
    await flush();

    await activation.deactivate({ code: "runtime-requested", description: "migration lost CAS" });

    expect(catalog.get(id)).toBeUndefined();
    expect(counters.disposed).toEqual([activation.instance]);
    expect(counters.unbound).toEqual([activation.instance]);
    expect(counters.unregistered).toEqual([activation]);
  });

  it("a failed activation leaves no invalid entry and is disposed and unbound once", async () => {
    resetProbe();
    const { catalog, counters } = buildCatalog();
    const id = new GrainId(failingMeta.grainType, "d");
    const activation = await catalog.getOrCreate(id);
    await expect(activation.invoke(ping(id, "r-d"))).rejects.toThrow("activation boom");
    await flush();

    expect(catalog.get(id)).toBeUndefined();
    expect(counters.disposed).toEqual([activation.instance]);
    expect(counters.unbound).toEqual([activation.instance]);
    expect(counters.unregistered).toEqual([activation]);
  });

  it("a deactivation requested during activation leaves no invalid entry and is disposed and unbound once", async () => {
    resetProbe();
    const { catalog, counters } = buildCatalog();
    const id = new GrainId(selfMeta.grainType, "e");
    const activation = await catalog.getOrCreate(id);
    await expect(activation.invoke(ping(id, "r-e"))).rejects.toThrow("activation unavailable");
    await flush();

    expect(catalog.get(id)).toBeUndefined();
    expect(counters.disposed).toEqual([activation.instance]);
    expect(counters.unbound).toEqual([activation.instance]);
    expect(counters.unregistered).toEqual([activation]);
  });
});
