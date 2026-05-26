import { describe, expect, it } from "vitest";
import type { GrainMetadata } from "@tsva/core/grain-metadata";
import { SiloAddress } from "@tsva/core/silo-address";
import { ActivationCountPlacement } from "@tsva/runtime/placement/activation-count-placement";
import { MetadataMatchFilter } from "@tsva/runtime/placement/metadata-match-filter";
import {
  placementFiltersFor,
  placementStrategyFor,
} from "@tsva/runtime/placement/placement-director";
import type { PlacementContext } from "@tsva/runtime/placement/placement-strategy";
import { PreferLocalPlacement } from "@tsva/runtime/placement/prefer-local-placement";
import { RandomPlacement } from "@tsva/runtime/placement/random-placement";
import { ResourceOptimizedPlacement } from "@tsva/runtime/placement/resource-optimized-placement";
import { SiloRoleBasedPlacement } from "@tsva/runtime/placement/silo-role-based-placement";
import { StatelessWorkerPlacement } from "@tsva/runtime/placement/stateless-worker-placement";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);
const candidates = [silo(0), silo(1), silo(2)];
const meta = (options: Partial<GrainMetadata["options"]> = {}): GrainMetadata => ({
  grainType: "Counter",
  options,
  reentrant: false,
  implicitSubscriptions: [],
  broadcastSubscriptions: [],
});

/** A `siloMetadata` accessor backed by a `ringKey -> metadata` map. */
const metadataOf =
  (table: Record<string, Record<string, string>>) =>
  (s: SiloAddress): Readonly<Record<string, string>> | undefined =>
    table[s.ringKey];

describe("RandomPlacement", () => {
  it("picks the candidate the RNG points at", () => {
    const ctx: PlacementContext = { localSilo: silo(0), random: () => 0.5 }; // -> index 1
    expect(new RandomPlacement().choose("Counter", candidates, ctx).ringKey).toBe("silo-1");
  });

  it("eventually selects every candidate", () => {
    const p = new RandomPlacement();
    const seen = new Set<string>();
    for (let i = 0; i < 200; i++) {
      seen.add(p.choose("Counter", candidates, { localSilo: silo(0) }).ringKey);
    }
    expect(seen.size).toBe(3);
  });

  it("throws when there are no candidates", () => {
    expect(() => new RandomPlacement().choose("Counter", [], { localSilo: silo(0) })).toThrow();
  });
});

describe("PreferLocalPlacement", () => {
  it("places on the local silo when it is a candidate", () => {
    const ctx: PlacementContext = { localSilo: silo(2) };
    expect(new PreferLocalPlacement().choose("Counter", candidates, ctx).ringKey).toBe("silo-2");
  });

  it("falls back to random when the local silo is not a candidate", () => {
    const ctx: PlacementContext = { localSilo: silo(9), random: () => 0 };
    expect(new PreferLocalPlacement().choose("Counter", candidates, ctx).ringKey).toBe("silo-0");
  });
});

describe("ActivationCountPlacement", () => {
  it("picks the least loaded among the sampled silos", () => {
    const load = new Map([
      ["silo-0", 10],
      ["silo-1", 3],
      ["silo-2", 7],
    ]);
    // k = 3 samples all candidates, so the global minimum (silo-1) wins.
    const p = new ActivationCountPlacement(3);
    const ctx: PlacementContext = {
      localSilo: silo(0),
      random: () => 0,
      activationCount: (s) => load.get(s.ringKey) ?? 0,
    };
    expect(p.choose("Counter", candidates, ctx).ringKey).toBe("silo-1");
  });
});

describe("StatelessWorkerPlacement", () => {
  it("always resolves to the local silo", () => {
    const ctx: PlacementContext = { localSilo: silo(1) };
    expect(new StatelessWorkerPlacement().choose("Counter", candidates, ctx).ringKey).toBe(
      "silo-1",
    );
  });
});

describe("MetadataMatchFilter", () => {
  it("keeps only silos matching every required key", () => {
    const ctx: PlacementContext = {
      localSilo: silo(0),
      siloMetadata: metadataOf({ "silo-0": { role: "worker" }, "silo-1": { role: "gateway" } }),
    };
    const kept = new MetadataMatchFilter({ role: "worker" }).filter("Counter", candidates, ctx);
    expect(kept.map((s) => s.ringKey)).toEqual(["silo-0"]);
  });

  it("requires all keys to match", () => {
    const ctx: PlacementContext = {
      localSilo: silo(0),
      siloMetadata: metadataOf({
        "silo-0": { role: "worker", zone: "a" },
        "silo-1": { role: "worker", zone: "b" },
      }),
    };
    const kept = new MetadataMatchFilter({ role: "worker", zone: "a" }).filter(
      "Counter",
      candidates,
      ctx,
    );
    expect(kept.map((s) => s.ringKey)).toEqual(["silo-0"]);
  });

  it("prunes silos with no metadata and returns empty when none match", () => {
    const ctx: PlacementContext = { localSilo: silo(0) }; // no siloMetadata accessor
    expect(new MetadataMatchFilter({ role: "worker" }).filter("Counter", candidates, ctx)).toEqual(
      [],
    );
  });
});

describe("SiloRoleBasedPlacement", () => {
  const ctx = (random?: () => number): PlacementContext => ({
    localSilo: silo(0),
    siloMetadata: metadataOf({
      "silo-0": { role: "gateway" },
      "silo-1": { role: "worker" },
      "silo-2": { role: "worker" },
    }),
    ...(random !== undefined ? { random } : {}),
  });

  it("routes only to silos advertising the role", () => {
    // random 0 -> first of the matched [silo-1, silo-2]
    expect(
      new SiloRoleBasedPlacement("worker").choose(
        "Counter",
        candidates,
        ctx(() => 0),
      ).ringKey,
    ).toBe("silo-1");
    expect(
      new SiloRoleBasedPlacement("worker").choose(
        "Counter",
        candidates,
        ctx(() => 0.99),
      ).ringKey,
    ).toBe("silo-2");
  });

  it("throws when no silo has the role", () => {
    expect(() =>
      new SiloRoleBasedPlacement("missing").choose("Counter", candidates, ctx()),
    ).toThrow();
  });
});

describe("ResourceOptimizedPlacement", () => {
  it("picks the silo with the lowest activation count", () => {
    const stats = new Map([
      ["silo-0", 10],
      ["silo-1", 2],
      ["silo-2", 7],
    ]);
    const ctx: PlacementContext = {
      localSilo: silo(0),
      resourceStats: (s) => ({ activationCount: stats.get(s.ringKey) ?? 0 }),
    };
    expect(new ResourceOptimizedPlacement().choose("Counter", candidates, ctx).ringKey).toBe(
      "silo-1",
    );
  });

  it("falls back to activationCount when resourceStats is absent", () => {
    const load = new Map([
      ["silo-0", 5],
      ["silo-1", 9],
      ["silo-2", 1],
    ]);
    const ctx: PlacementContext = {
      localSilo: silo(0),
      activationCount: (s) => load.get(s.ringKey) ?? 0,
    };
    expect(new ResourceOptimizedPlacement().choose("Counter", candidates, ctx).ringKey).toBe(
      "silo-2",
    );
  });

  it("breaks ties by preferring the local silo", () => {
    const ctx: PlacementContext = {
      localSilo: silo(2),
      resourceStats: () => ({ activationCount: 0 }), // all equal
    };
    expect(new ResourceOptimizedPlacement().choose("Counter", candidates, ctx).ringKey).toBe(
      "silo-2",
    );
  });
});

describe("placementStrategyFor", () => {
  it("maps metadata to the right strategy, defaulting to random", () => {
    expect(placementStrategyFor(meta())).toBeInstanceOf(RandomPlacement);
    expect(placementStrategyFor(meta({ placement: "random" }))).toBeInstanceOf(RandomPlacement);
    expect(placementStrategyFor(meta({ placement: "preferLocal" }))).toBeInstanceOf(
      PreferLocalPlacement,
    );
    expect(placementStrategyFor(meta({ placement: "activationCount" }))).toBeInstanceOf(
      ActivationCountPlacement,
    );
    expect(placementStrategyFor(meta({ placement: "siloRole", role: "worker" }))).toBeInstanceOf(
      SiloRoleBasedPlacement,
    );
    expect(placementStrategyFor(meta({ placement: "resourceOptimized" }))).toBeInstanceOf(
      ResourceOptimizedPlacement,
    );
    expect(placementStrategyFor(meta({ stateless: true }))).toBeInstanceOf(
      StatelessWorkerPlacement,
    );
  });

  it("requires a role for siloRole placement", () => {
    expect(() => placementStrategyFor(meta({ placement: "siloRole" }))).toThrow();
  });

  it("stateless takes precedence over a placement value", () => {
    expect(
      placementStrategyFor(meta({ stateless: true, placement: "resourceOptimized" })),
    ).toBeInstanceOf(StatelessWorkerPlacement);
  });
});

describe("placementFiltersFor", () => {
  it("returns no filters by default", () => {
    expect(placementFiltersFor(meta())).toEqual([]);
  });

  it("builds a MetadataMatchFilter from a descriptor", () => {
    const filters = placementFiltersFor(
      meta({ placementFilters: [{ kind: "metadataMatch", match: { role: "worker" } }] }),
    );
    expect(filters).toHaveLength(1);
    expect(filters[0]).toBeInstanceOf(MetadataMatchFilter);
    const ctx: PlacementContext = {
      localSilo: silo(0),
      siloMetadata: metadataOf({ "silo-1": { role: "worker" } }),
    };
    expect(filters[0]!.filter("Counter", candidates, ctx).map((s) => s.ringKey)).toEqual([
      "silo-1",
    ]);
  });

  it("returns no filters for stateless workers", () => {
    expect(
      placementFiltersFor(
        meta({
          stateless: true,
          placementFilters: [{ kind: "metadataMatch", match: { role: "x" } }],
        }),
      ),
    ).toEqual([]);
  });
});
