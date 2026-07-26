// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/ResourceOptimizedPlacementOptionsTests.cs @ v10.1.0 (MIT).
import { describe, expect, it } from "vitest";
import {
  DEFAULT_ACTIVATION_COUNT_WEIGHT,
  DEFAULT_AVAILABLE_MEMORY_WEIGHT,
  DEFAULT_CPU_USAGE_WEIGHT,
  DEFAULT_MAX_AVAILABLE_MEMORY_WEIGHT,
  DEFAULT_MEMORY_USAGE_WEIGHT,
  defaultResourceOptimizedPlacementOptions,
  ResourceOptimizedPlacementOptionsValidator,
  type ResourceOptimizedPlacementOptions,
} from "@thresh/runtime/placement/resource-optimized-placement-options";

const optionsWith = (
  overrides: Partial<ResourceOptimizedPlacementOptions>,
): ResourceOptimizedPlacementOptions => ({
  cpuUsageWeight: 0,
  memoryUsageWeight: 0,
  availableMemoryWeight: 0,
  maxAvailableMemoryWeight: 0,
  activationCountWeight: 0,
  localSiloPreferenceMargin: 0,
  ...overrides,
});

describe("UnitTests.General.ResourceOptimizedPlacementOptionsTests", () => {
  it("ConstantsShouldNotChange", () => {
    expect(DEFAULT_CPU_USAGE_WEIGHT).toBe(40);
    expect(DEFAULT_MEMORY_USAGE_WEIGHT).toBe(20);
    expect(DEFAULT_AVAILABLE_MEMORY_WEIGHT).toBe(20);
    expect(DEFAULT_MAX_AVAILABLE_MEMORY_WEIGHT).toBe(5);
    expect(DEFAULT_ACTIVATION_COUNT_WEIGHT).toBe(15);
  });

  it("DefaultShouldEqualConstants", () => {
    const options = defaultResourceOptimizedPlacementOptions();
    expect(options.cpuUsageWeight).toBe(DEFAULT_CPU_USAGE_WEIGHT);
    expect(options.memoryUsageWeight).toBe(DEFAULT_MEMORY_USAGE_WEIGHT);
    expect(options.availableMemoryWeight).toBe(DEFAULT_AVAILABLE_MEMORY_WEIGHT);
    expect(options.maxAvailableMemoryWeight).toBe(DEFAULT_MAX_AVAILABLE_MEMORY_WEIGHT);
    expect(options.activationCountWeight).toBe(DEFAULT_ACTIVATION_COUNT_WEIGHT);
  });

  it.each([
    optionsWith({ cpuUsageWeight: -10 }),
    optionsWith({ cpuUsageWeight: 101 }),
    optionsWith({ memoryUsageWeight: -10 }),
    optionsWith({ memoryUsageWeight: 101 }),
    optionsWith({ availableMemoryWeight: -10 }),
    optionsWith({ availableMemoryWeight: 101 }),
    optionsWith({ maxAvailableMemoryWeight: -10 }),
    optionsWith({ maxAvailableMemoryWeight: 101 }),
    optionsWith({ activationCountWeight: -10 }),
    optionsWith({ activationCountWeight: 101 }),
    optionsWith({ localSiloPreferenceMargin: -10 }),
    optionsWith({ localSiloPreferenceMargin: 101 }),
  ])("InvalidWeightsShouldThrow (%#)", (options) => {
    const validator = new ResourceOptimizedPlacementOptionsValidator(options);
    expect(() => validator.validateConfiguration()).toThrow();
  });

  it.each([
    optionsWith({ cpuUsageWeight: 10, localSiloPreferenceMargin: 10 }),
    optionsWith({ cpuUsageWeight: 100, localSiloPreferenceMargin: 10 }),
    optionsWith({ cpuUsageWeight: 10, memoryUsageWeight: 10 }),
    optionsWith({ cpuUsageWeight: 10, memoryUsageWeight: 100 }),
    optionsWith({ cpuUsageWeight: 10, availableMemoryWeight: 10 }),
    optionsWith({ cpuUsageWeight: 10, availableMemoryWeight: 100 }),
    optionsWith({ cpuUsageWeight: 10, maxAvailableMemoryWeight: 10 }),
    optionsWith({ cpuUsageWeight: 10, maxAvailableMemoryWeight: 100 }),
    optionsWith({ cpuUsageWeight: 10, activationCountWeight: 10 }),
    optionsWith({ cpuUsageWeight: 10, activationCountWeight: 100 }),
    optionsWith({ cpuUsageWeight: 10, localSiloPreferenceMargin: 100 }),
  ])("ValidWeightsShouldNotThrow (%#)", (options) => {
    const validator = new ResourceOptimizedPlacementOptionsValidator(options);
    expect(() => validator.validateConfiguration()).not.toThrow();
  });
});
