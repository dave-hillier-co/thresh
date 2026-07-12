// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/RebalancingOptionsTests.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import {
  ActivationRebalancerOptionsValidator,
  DEFAULT_REBALANCER_DUE_TIME_MS,
  DEFAULT_SESSION_CYCLE_PERIOD_MS,
  defaultActivationRebalancerOptions,
  type ActivationRebalancerOptions,
  type DeploymentLoadPublisherOptions,
} from "@tsva/runtime/placement/rebalancing/rebalancer-options";
import {
  DEFAULT_ACTIVATION_MIGRATION_COUNT_LIMIT,
  DEFAULT_ALLOWED_ENTROPY_DEVIATION,
  DEFAULT_CYCLE_NUMBER_WEIGHT,
  DEFAULT_ENTROPY_QUANTUM,
  DEFAULT_MAX_STAGNANT_CYCLES,
  DEFAULT_SCALE_ALLOWED_ENTROPY_DEVIATION,
  DEFAULT_SCALED_ENTROPY_DEVIATION_ACTIVATION_THRESHOLD,
  DEFAULT_SILO_NUMBER_WEIGHT,
  MAX_SCALED_ENTROPY_DEVIATION,
} from "@tsva/runtime/placement/rebalancing/rebalancer-model";

describe("UnitTests.ActivationRebalancingTests.RebalancingOptionsTests", () => {
  orleansTest("UnitTests.ActivationRebalancingTests.RebalancingOptionsTests.ConstantsShouldNotChange", () => {
    expect(DEFAULT_REBALANCER_DUE_TIME_MS).toBe(60_000);
    expect(DEFAULT_SESSION_CYCLE_PERIOD_MS).toBe(15_000);
    expect(DEFAULT_MAX_STAGNANT_CYCLES).toBe(3);
    expect(DEFAULT_ENTROPY_QUANTUM).toBe(0.0001);
    expect(DEFAULT_ALLOWED_ENTROPY_DEVIATION).toBe(0.0001);
    expect(DEFAULT_CYCLE_NUMBER_WEIGHT).toBe(0.1);
    expect(DEFAULT_SILO_NUMBER_WEIGHT).toBe(0.1);
    expect(MAX_SCALED_ENTROPY_DEVIATION).toBe(0.1);
    expect(DEFAULT_SCALED_ENTROPY_DEVIATION_ACTIVATION_THRESHOLD).toBe(10_000);
    expect(DEFAULT_ACTIVATION_MIGRATION_COUNT_LIMIT).toBe(Number.MAX_SAFE_INTEGER);
    expect(DEFAULT_SCALE_ALLOWED_ENTROPY_DEVIATION).toBe(true);
  });

  orleansTest.each([
    // [sessionCyclePeriodMs, publisherRefreshTimeMs, maxStagnantCycles, entropyQuantum,
    //  allowedEntropyDeviation, cycleNumberWeight, siloNumberWeight,
    //  activationMigrationCountLimit, scaledEntropyDeviationActivationThreshold]
    [1000, 1000, 0, 0.2, 0.2, 0, -0.1, 0, 999],
    [2000, 2000, -1, 0.05, 0.05, 0.5, 1.1, 10, 500],
    [1000, 1000, 2, 0, 0.05, 0.5, 0.5, 10, 999],
    [1000, 1000, 2, 0.05, 0, 0.5, 0.5, 10, 999],
    [1000, 1000, 2, 0.05, 0.05, -0.1, 0.5, 10, 999],
    [1000, 1000, 2, 0.05, 0.05, 0.5, 1.1, 10, 999],
    [1000, 1000, 2, 0.05, 0.05, 0.5, 0.5, 0, 999],
    [1000, 1000, 2, 0.05, 0.05, 0.5, 0.5, 10, 999],
  ] as const)(
    "UnitTests.ActivationRebalancingTests.RebalancingOptionsTests.InvalidOptionsShouldThrow",
    (item) => {
      const [
        sessionCyclePeriodMs,
        publisherRefreshTimeMs,
        maxStagnantCycles,
        entropyQuantum,
        allowedEntropyDeviation,
        cycleNumberWeight,
        siloNumberWeight,
        activationMigrationCountLimit,
        scaledEntropyDeviationActivationThreshold,
      ] = item;
      const publisherOptions: DeploymentLoadPublisherOptions = {
        deploymentLoadPublisherRefreshTimeMs: publisherRefreshTimeMs,
      };
      const options: ActivationRebalancerOptions = {
        ...defaultActivationRebalancerOptions(),
        sessionCyclePeriodMs,
        maxStagnantCycles,
        entropyQuantum,
        allowedEntropyDeviation,
        cycleNumberWeight,
        siloNumberWeight,
        activationMigrationCountLimit,
        scaledEntropyDeviationActivationThreshold,
      };

      const validator = new ActivationRebalancerOptionsValidator(options, publisherOptions);
      expect(() => validator.validateConfiguration()).toThrow();
    },
  );
});
