import { afterEach, describe, expect, it } from "vitest";
import {
  BROADCAST_CHANNEL_OBSERVER,
  type BroadcastChannelHandler,
} from "@thresh/core/broadcast-channel";
import { grain, mayInterleave } from "@thresh/core/decorators";
import {
  defineGrain,
  useDependency,
  useGrainFacet,
  useMayInterleave,
  useOnActivate,
  useOnDehydrate,
  useOnRehydrate,
} from "@thresh/core/define-grain";
import { Grain } from "@thresh/core/grain";
import { createDependencyToken } from "@thresh/core/grain-dependency";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";

let cluster: TestCluster | undefined;

afterEach(async () => {
  await cluster?.dispose();
  cluster = undefined;
});

const tick = (): Promise<void> => new Promise((r) => setTimeout(r, 5));

// ---------------------------------------------------------------- may-interleave

interface IGated extends GrainKey<string> {
  park(): Promise<string>;
  release(): Promise<void>;
  poke(): Promise<string>;
  queued(arg?: string): Promise<string>;
}
const IFnGated = defineGrainInterface<IGated>("IFnGated.authoring");
const IClassGated = defineGrainInterface<IGated>("IClassGated.authoring");

/** One shared gate per scenario: `park` waits on it, `release` opens it. */
function newGate(): { wait(): Promise<void>; open(): void } {
  let open!: () => void;
  const opened = new Promise<void>((r) => (open = r));
  return { wait: () => opened, open };
}
let gate = newGate();

const interleaves = (method: string, args: readonly unknown[]): boolean =>
  method === "release" || method === "poke" || (method === "queued" && args[0] === "now");

const FnGated = defineGrain<IGated>("FnGated", () => {
  useMayInterleave(interleaves);
  return {
    park: async () => {
      await gate.wait();
      return "parked";
    },
    release: async () => gate.open(),
    poke: async () => "poked",
    queued: async () => "queued",
  };
});

@grain({ name: "ClassGated" })
@mayInterleave(interleaves)
class ClassGated extends Grain implements IGated {
  async park(): Promise<string> {
    await gate.wait();
    return "parked";
  }
  async release(): Promise<void> {
    gate.open();
  }
  async poke(): Promise<string> {
    return "poked";
  }
  async queued(): Promise<string> {
    return "queued";
  }
}

describe("useMayInterleave", () => {
  const scenarios = [
    ["functional", IFnGated, { ctor: FnGated.grain, interfaces: [IFnGated] }],
    ["class", IClassGated, { ctor: ClassGated, interfaces: [IClassGated] }],
  ] as const;

  for (const [label, iface, spec] of scenarios) {
    it(`admits the calls the predicate allows and queues the rest (${label})`, async () => {
      gate = newGate();
      cluster = await TestCluster.start({ initialSilos: 1, grains: [spec] });
      const g = cluster.getGrain(iface, "a");

      const parked = g.park();
      await tick();
      // Admitted while `park` holds the turn.
      expect(await g.poke()).toBe("poked");
      // Admitted only for the argument the predicate inspects.
      const blocked = g.queued("later");
      const admitted = g.queued("now");
      expect(await admitted).toBe("queued");
      let blockedDone = false;
      void blocked.then(() => (blockedDone = true));
      await tick();
      expect(blockedDone).toBe(false);

      await g.release();
      expect(await parked).toBe("parked");
      expect(await blocked).toBe("queued");
    });
  }
});

// ---------------------------------------------------------------- facets

interface IFaceted extends GrainKey<string> {
  label(): Promise<string>;
  activatedWith(): Promise<string>;
}
const IFnFaceted = defineGrainInterface<IFaceted>("IFnFaceted.authoring");

interface WidgetFacet {
  readonly label: string;
}
let facetLoads = 0;

const FnFaceted = defineGrain<IFaceted>("FnFaceted", () => {
  const widget = useGrainFacet<WidgetFacet>("widget", { name: "fancy", config: { tag: "cfg" } });
  let seenOnActivate = "";
  useOnActivate(() => {
    seenOnActivate = widget.get().label;
  });
  return {
    label: async () => widget.get().label,
    activatedWith: async () => seenOnActivate,
  };
});

describe("useGrainFacet", () => {
  it("binds a third-party facet from the builder's registry before onActivate", async () => {
    facetLoads = 0;
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: FnFaceted.grain, interfaces: [IFnFaceted] }],
      configureSilo: (builder) => {
        builder.addGrainFacetFactory("widget", "fancy", (config, grainId) => {
          facetLoads += 1;
          return { label: `${(config as { tag: string }).tag}:${String(grainId.key)}` };
        });
      },
    });
    const g = cluster.getGrain(IFnFaceted, "k");
    expect(await g.label()).toBe("cfg:k");
    expect(await g.activatedWith()).toBe("cfg:k");
    expect(facetLoads).toBe(1);
  });
});

// ---------------------------------------------------------------- regex channels

interface IChannelSink extends GrainKey<string> {
  received(): Promise<string[]>;
}
const IFnChannelSink = defineGrainInterface<IChannelSink>("IFnChannelSink.authoring");

const FnChannelSink = defineGrain<IChannelSink>(
  "FnChannelSink",
  () => {
    const items: string[] = [];
    return {
      received: async () => items,
      [BROADCAST_CHANNEL_OBSERVER]: (namespace: string): BroadcastChannelHandler<unknown> => ({
        onPublished: (item) => {
          items.push(`${namespace}:${String(item)}`);
        },
      }),
    };
  },
  { regexImplicitChannelSubscriptions: ["fn-regex-(.)+"] },
);

describe("regexImplicitChannelSubscriptions", () => {
  it("delivers items published to a matching namespace and ignores others", async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: FnChannelSink.grain, interfaces: [IFnChannelSink] }],
      configureSilo: (builder) => {
        builder.useBroadcastChannels("bc", { fireAndForgetDelivery: false });
      },
    });
    const provider = (await cluster.client).getBroadcastChannelProvider("bc");
    await provider.getChannelWriter<string>({ namespace: "fn-regex-1", key: "k" }).publish("hit");
    await provider.getChannelWriter<string>({ namespace: "other", key: "k" }).publish("miss");
    const g = cluster.getGrain(IFnChannelSink, "k");
    await waitFor(async () => (await g.received()).length === 1);
    expect(await g.received()).toEqual(["fn-regex-1:hit"]);
  });
});

// ---------------------------------------------------------------- migration

interface IMigrant extends GrainKey<string> {
  increment(by: number): Promise<number>;
  get(): Promise<number>;
  rehydratedFrom(): Promise<string>;
  scheduleMigration(target?: SiloAddress): Promise<void>;
}
const IFnMigrant = defineGrainInterface<IMigrant>("IFnMigrant.authoring");

const FnMigrant = defineGrain<IMigrant>("FnMigrant", (ctx) => {
  let count = 0;
  let from = "";
  useOnDehydrate((bag) => {
    bag.set("count", count);
    bag.set("from", String(ctx.runtime.localSiloAddress()));
  });
  useOnRehydrate((bag) => {
    count = bag.get<number>("count") ?? -1;
    from = bag.get<string>("from") ?? "";
  });
  return {
    increment: async (by: number) => (count += by),
    get: async () => count,
    rehydratedFrom: async () => from,
    scheduleMigration: async (target?: SiloAddress) => ctx.runtime.migrateOnIdle(target),
  };
});

const migrantId = new GrainId("FnMigrant", "x");

describe("useOnDehydrate / useOnRehydrate", () => {
  it("carries state set in the dehydrate hook into the rehydrate hook after a migration", async () => {
    const time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      clusterId: "c-fn-migration",
      initialSilos: 2,
      time,
      collectionAgeSeconds: 30,
      collectionIntervalSeconds: 10,
      random: () => 0,
      grains: [{ ctor: FnMigrant.grain, interfaces: [IFnMigrant] }],
    });
    const [s0, s1] = cluster.silos;
    expect(await s1!.host.getGrain(IFnMigrant, "x").increment(5)).toBe(5);
    expect(s0!.host.isActive(migrantId)).toBe(true);
    await s1!.host.getGrain(IFnMigrant, "x").scheduleMigration(s1!.address);

    time.advance(31_000);
    await waitFor(() => s1!.host.isActive(migrantId));

    const migrated = s0!.host.getGrain(IFnMigrant, "x");
    expect(await migrated.get()).toBe(5);
    expect(await migrated.rehydratedFrom()).toBe(String(s0!.address));
  });

  it("does not expose the hooks as remotely callable methods", async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: FnMigrant.grain, interfaces: [IFnMigrant] }],
    });
    const g = cluster.getGrain(IFnMigrant, "y") as unknown as Record<
      string,
      (...args: unknown[]) => Promise<unknown>
    >;
    await expect(g.onDehydrate!({})).rejects.toThrow(/has no method onDehydrate/);
    await expect(g.onRehydrate!({})).rejects.toThrow(/has no method onRehydrate/);
  });
});

// ---------------------------------------------------------------- dependencies

interface IWired extends GrainKey<string> {
  region(): Promise<string>;
}
const IFnWired = defineGrainInterface<IWired>("IFnWired.authoring");
const REGION = createDependencyToken<string>("region");

const FnWired = defineGrain<IWired>(
  "FnWired",
  () => {
    const region = useDependency(REGION);
    return { region: async () => region };
  },
  { placement: "preferLocal" },
);

describe("useDependency", () => {
  it("hands the factory the value registered on its own silo's builder", async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: FnWired.grain, interfaces: [IFnWired] }],
      configureSilo: (builder, silo) => {
        builder.addGrainDependency(REGION, `region-${silo.index}`);
      },
    });
    const [s0, s1] = cluster.silos;
    expect(await s0!.host.getGrain(IFnWired, "a").region()).toBe("region-0");
    expect(await s1!.host.getGrain(IFnWired, "b").region()).toBe("region-1");
  });

  it("fails the activation naming the token when no silo registered it", async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: FnWired.grain, interfaces: [IFnWired] }],
    });
    await expect(cluster.getGrain(IFnWired, "a").region()).rejects.toThrow(/region/);
  });
});
