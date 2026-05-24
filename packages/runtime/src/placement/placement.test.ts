import { describe, expect, it } from "vitest";
import type { GrainMetadata } from "@tsva/core/grain-metadata";
import { SiloAddress } from "@tsva/core/silo-address";
import { ActivationCountPlacement } from "@tsva/runtime/placement/activation-count-placement";
import { placementStrategyFor } from "@tsva/runtime/placement/placement-director";
import type { PlacementContext } from "@tsva/runtime/placement/placement-strategy";
import { PreferLocalPlacement } from "@tsva/runtime/placement/prefer-local-placement";
import { RandomPlacement } from "@tsva/runtime/placement/random-placement";
import { StatelessWorkerPlacement } from "@tsva/runtime/placement/stateless-worker-placement";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);
const candidates = [silo(0), silo(1), silo(2)];
const meta = (options: Partial<GrainMetadata["options"]> = {}): GrainMetadata => ({
  grainType: "Counter",
  options,
  reentrant: false,
});

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
    expect(placementStrategyFor(meta({ stateless: true }))).toBeInstanceOf(
      StatelessWorkerPlacement,
    );
  });
});
