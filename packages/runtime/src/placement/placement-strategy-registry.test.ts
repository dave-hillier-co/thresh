import type { GrainMetadata } from "@thresh/core/grain-metadata";
import { SiloAddress } from "@thresh/core/silo-address";
import { describe, expect, it } from "vitest";

import { PlacementStrategyRegistry } from "./placement-strategy-registry";
import { placementStrategyFor } from "./placement-director";
import { RandomPlacement } from "./random-placement";
import { StatelessWorkerPlacement } from "./stateless-worker-placement";
import type { PlacementStrategy } from "./placement-strategy";

const silo = (n: number): SiloAddress => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);
const localSilo = silo(1);

function metadata(options: GrainMetadata["options"]): GrainMetadata {
  return { grainType: "Shard", options } as GrainMetadata;
}

// A director that pins every grain to one silo, standing in for Orleans'
// IPlacementDirector — the seam Spiceport's GraphLocalityPlacementDirector needs.
const pinned: PlacementStrategy = {
  choose: (_type, candidates) => candidates[candidates.length - 1]!,
};

describe("PlacementStrategyRegistry", () => {
  it("resolves a director registered under a name", () => {
    const registry = new PlacementStrategyRegistry().add("graphLocality", pinned);

    expect(registry.resolve("graphLocality")).toBe(pinned);
  });

  it("throws for a name nothing registered", () => {
    expect(() => new PlacementStrategyRegistry().resolve("missing")).toThrow(
      /graphLocality|missing/,
    );
  });
});

describe("placementStrategyFor with a custom strategy", () => {
  it("resolves the named director from the registry", () => {
    const registry = new PlacementStrategyRegistry().add("graphLocality", pinned);
    const strategy = placementStrategyFor(
      metadata({ placement: "custom", strategy: "graphLocality" }),
      registry,
    );

    expect(strategy).toBe(pinned);
    const other = silo(2);
    expect(strategy.choose("Shard", [localSilo, other], { localSilo })).toBe(other);
  });

  it("throws when a custom placement names no strategy", () => {
    expect(() => placementStrategyFor(metadata({ placement: "custom" }))).toThrow(/strategy/);
  });

  it("throws when the registry is absent or does not have the director", () => {
    expect(() =>
      placementStrategyFor(metadata({ placement: "custom", strategy: "graphLocality" })),
    ).toThrow(/graphLocality/);
    expect(() =>
      placementStrategyFor(
        metadata({ placement: "custom", strategy: "graphLocality" }),
        new PlacementStrategyRegistry(),
      ),
    ).toThrow(/graphLocality/);
  });

  // Stateless-worker placement is decided before the strategy switch in Orleans, and a
  // stateless worker is always local, so a custom strategy must not displace it.
  it("still yields stateless-worker placement when both are declared", () => {
    const registry = new PlacementStrategyRegistry().add("graphLocality", pinned);
    const strategy = placementStrategyFor(
      metadata({ stateless: true, placement: "custom", strategy: "graphLocality" }),
      registry,
    );

    expect(strategy).toBeInstanceOf(StatelessWorkerPlacement);
  });

  it("leaves the built-in strategies untouched", () => {
    expect(placementStrategyFor(metadata({}))).toBeInstanceOf(RandomPlacement);
    expect(placementStrategyFor(metadata({ placement: "random" }))).toBeInstanceOf(RandomPlacement);
  });
});
