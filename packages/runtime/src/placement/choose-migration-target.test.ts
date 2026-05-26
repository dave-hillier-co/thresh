import { describe, expect, it } from "vitest";
import { SiloAddress } from "@tsva/core/silo-address";
import { chooseMigrationTarget } from "@tsva/runtime/placement/choose-migration-target";
import type { PlacementContext } from "@tsva/runtime/placement/placement-strategy";
import { RandomPlacement } from "@tsva/runtime/placement/random-placement";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);
const candidates = [silo(0), silo(1), silo(2)];
const strategy = new RandomPlacement();
// random -> 0 picks the first of the "others" list (which excludes the local silo).
const ctx = (local: SiloAddress): PlacementContext => ({ localSilo: local, random: () => 0 });

describe("chooseMigrationTarget", () => {
  it("honours a directed target that is a live, non-local candidate", () => {
    const target = chooseMigrationTarget("Counter", silo(2), candidates, strategy, ctx(silo(0)));
    expect(target?.ringKey).toBe("silo-2");
  });

  it("falls back to the strategy (over other silos) when the directed target is dead", () => {
    const target = chooseMigrationTarget("Counter", silo(9), candidates, strategy, ctx(silo(0)));
    expect(target?.ringKey).toBe("silo-1"); // others = [silo-1, silo-2], random->0 picks silo-1
  });

  it("never returns the local silo (it is excluded from the strategy candidates)", () => {
    const target = chooseMigrationTarget("Counter", undefined, candidates, strategy, ctx(silo(0)));
    expect(target?.ringKey).toBe("silo-1");
    expect(target?.equals(silo(0))).toBe(false);
  });

  it("ignores a directed target that is the local silo and uses the strategy", () => {
    const target = chooseMigrationTarget("Counter", silo(0), candidates, strategy, ctx(silo(0)));
    expect(target?.ringKey).toBe("silo-1");
  });

  it("returns undefined when the local silo is the only candidate", () => {
    expect(
      chooseMigrationTarget("Counter", undefined, [silo(0)], strategy, ctx(silo(0))),
    ).toBeUndefined();
    expect(
      chooseMigrationTarget("Counter", silo(1), [silo(0)], strategy, ctx(silo(0))),
    ).toBeUndefined();
  });
});
