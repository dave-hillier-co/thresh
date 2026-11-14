import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import type {
  PlacementContext,
  SiloResourceStats,
} from "@thresh/runtime/placement/placement-strategy";
import { ResourceOptimizedPlacement } from "@thresh/runtime/placement/resource-optimized-placement";
import {
  defaultResourceOptimizedPlacementOptions,
  type ResourceOptimizedPlacementOptions,
} from "@thresh/runtime/placement/resource-optimized-placement-options";

const GIB = 1024 ** 3;
const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);
const pair = [silo(0), silo(1)];

const stats = (cpu: number, usedGib: number, activations = 10): SiloResourceStats => ({
  activationCount: activations,
  cpuUsagePercentage: cpu,
  memoryUsageBytes: usedGib * GIB,
  availableMemoryBytes: (8 - usedGib) * GIB,
  maximumAvailableMemoryBytes: 8 * GIB,
});

const context = (
  local: SiloAddress,
  table: Record<string, SiloResourceStats>,
): PlacementContext => ({
  localSilo: local,
  random: () => 0,
  resourceStats: (s) => table[s.ringKey],
});

const weights = (
  overrides: Partial<ResourceOptimizedPlacementOptions>,
): ResourceOptimizedPlacementOptions => ({
  ...defaultResourceOptimizedPlacementOptions(),
  ...overrides,
});

describe("ResourceOptimizedPlacement scoring (Orleans ResourceOptimizedPlacementDirector)", () => {
  it("prefers the silo with the lower CPU reading under the default weights", () => {
    const ctx = context(silo(1), { "silo-0": stats(10, 2), "silo-1": stats(90, 2) });
    expect(new ResourceOptimizedPlacement().choose("G", pair, ctx).ringKey).toBe("silo-0");
  });

  it("prefers the silo with less memory in use when only the memory weights are set", () => {
    const ctx = context(silo(0), { "silo-0": stats(50, 7), "silo-1": stats(50, 1) });
    const placement = new ResourceOptimizedPlacement(
      weights({
        cpuUsageWeight: 0,
        memoryUsageWeight: 50,
        availableMemoryWeight: 50,
        maxAvailableMemoryWeight: 0,
        activationCountWeight: 0,
      }),
    );
    expect(placement.choose("G", pair, ctx).ringKey).toBe("silo-1");
  });

  it("follows the weights: the same readings pick different silos under different weights", () => {
    // silo-0: low CPU, heavy memory. silo-1: high CPU, light memory.
    const ctx = context(silo(0), { "silo-0": stats(5, 7.5), "silo-1": stats(95, 0.5) });
    const cpuLed = new ResourceOptimizedPlacement(
      weights({ cpuUsageWeight: 100, memoryUsageWeight: 0, availableMemoryWeight: 0 }),
    );
    const memoryLed = new ResourceOptimizedPlacement(
      weights({ cpuUsageWeight: 0, memoryUsageWeight: 100, availableMemoryWeight: 100 }),
    );
    expect(cpuLed.choose("G", pair, ctx).ringKey).toBe("silo-0");
    expect(memoryLed.choose("G", pair, ctx).ringKey).toBe("silo-1");
  });

  it("keeps the local silo when it scores within the preference margin of the best", () => {
    const ctx = context(silo(1), { "silo-0": stats(40, 2), "silo-1": stats(42, 2) });
    expect(new ResourceOptimizedPlacement().choose("G", pair, ctx).ringKey).toBe("silo-1");
    const noMargin = new ResourceOptimizedPlacement(weights({ localSiloPreferenceMargin: 0 }));
    expect(noMargin.choose("G", pair, ctx).ringKey).toBe("silo-0");
  });

  it("does not score an overloaded silo", () => {
    const ctx = context(silo(1), {
      "silo-0": { ...stats(1, 1, 0), isOverloaded: true },
      "silo-1": stats(80, 6),
    });
    expect(new ResourceOptimizedPlacement().choose("G", pair, ctx).ringKey).toBe("silo-1");
  });

  it("falls back to a random candidate when no silo has reported statistics", () => {
    const ctx: PlacementContext = { localSilo: silo(0), random: () => 0.99 };
    expect(new ResourceOptimizedPlacement().choose("G", pair, ctx).ringKey).toBe("silo-1");
  });
});
