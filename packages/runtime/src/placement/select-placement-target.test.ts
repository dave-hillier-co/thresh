import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { ActivationCountPlacement } from "@thresh/runtime/placement/activation-count-placement";
import { MetadataMatchFilter } from "@thresh/runtime/placement/metadata-match-filter";
import type { PlacementContext } from "@thresh/runtime/placement/placement-strategy";
import { RandomPlacement } from "@thresh/runtime/placement/random-placement";
import { selectPlacementTarget } from "@thresh/runtime/placement/select-placement-target";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);
const all = [silo(0), silo(1), silo(2)];
const grainId = new GrainId("Counter", "a");

const baseContext = (extra: Partial<PlacementContext> = {}): PlacementContext => ({
  localSilo: silo(0),
  grainId,
  random: () => 0,
  ...extra,
});

describe("selectPlacementTarget", () => {
  it("never returns an excluded silo", async () => {
    const target = await selectPlacementTarget({
      grainType: "Counter",
      candidates: all,
      exclude: [silo(0)],
      filters: [],
      strategy: new RandomPlacement(),
      context: baseContext(),
    });
    expect(target?.ringKey).toBe("silo-1");
  });

  it("returns undefined when nothing remains after the exclude list", async () => {
    const target = await selectPlacementTarget({
      grainType: "Counter",
      candidates: [silo(0)],
      exclude: [silo(0)],
      filters: [],
      strategy: new RandomPlacement(),
      context: baseContext(),
    });
    expect(target).toBeUndefined();
  });

  it("honours a hint that names a surviving candidate", async () => {
    const target = await selectPlacementTarget({
      grainType: "Counter",
      candidates: all,
      exclude: [silo(0)],
      filters: [],
      strategy: new RandomPlacement(),
      context: baseContext(),
      hint: (candidates) => candidates.find((c) => c.equals(silo(2))),
    });
    expect(target?.ringKey).toBe("silo-2");
  });

  it("does not obey a hint naming a silo the filters excluded", async () => {
    const meta = new Map([
      ["silo-0", { zone: "a" }],
      ["silo-1", { zone: "a" }],
      ["silo-2", { zone: "b" }],
    ]);
    const target = await selectPlacementTarget({
      grainType: "Counter",
      candidates: all,
      exclude: [silo(0)],
      filters: [new MetadataMatchFilter({ zone: "a" })],
      strategy: new RandomPlacement(),
      context: baseContext({ siloMetadata: (s) => meta.get(s.ringKey) }),
      hint: (candidates) => candidates.find((c) => c.equals(silo(2))),
    });
    expect(target?.ringKey).toBe("silo-1");
  });

  it("lets a load-aware strategy see the context's real peer counts", async () => {
    const counts = new Map([
      ["silo-0", 9],
      ["silo-1", 7],
      ["silo-2", 1],
    ]);
    const target = await selectPlacementTarget({
      grainType: "Counter",
      candidates: all,
      exclude: [silo(0)],
      filters: [],
      strategy: new ActivationCountPlacement(),
      context: baseContext({ activationCount: (s) => counts.get(s.ringKey) ?? 0 }),
    });
    expect(target?.ringKey).toBe("silo-2");
  });

  it("prefers version-compatible candidates but falls back when none qualify", async () => {
    const compatible = await selectPlacementTarget({
      grainType: "Counter",
      candidates: all,
      filters: [],
      strategy: new RandomPlacement(),
      context: baseContext(),
      versionFilter: async () => [silo(2)],
    });
    expect(compatible?.ringKey).toBe("silo-2");
    const fallback = await selectPlacementTarget({
      grainType: "Counter",
      candidates: all,
      filters: [],
      strategy: new RandomPlacement(),
      context: baseContext(),
      versionFilter: async () => [],
    });
    expect(fallback?.ringKey).toBe("silo-0");
  });
});
