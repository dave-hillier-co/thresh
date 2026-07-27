// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRepartitioningTests/OptionsTests.cs @ v10.1.0 (MIT).
//
// `ConstantsShouldNotChange` pins the exact numeric defaults on
// `ActivationRepartitionerOptions` (`DEFAULT_ANCHORING_FILTER_ENABLED`,
// `DEFAULT_PROBABILISTIC_FILTERING_MAX_ALLOWED_ERROR`,
// `DEFAULT_MAX_EDGE_COUNT`, `DEFAULT_MINUMUM_ROUND_PERIOD`,
// `DEFAULT_MAXIMUM_ROUND_PERIOD`, `DEFAULT_RECOVERY_PERIOD`);
// `InvalidOptionsShouldThrow` exercises
// `ActivationRepartitionerOptionsValidator.ValidateConfiguration` across 8 bad
// combinations of `MaxEdgeCount`, `MaxUnprocessedEdges`, `MinRoundPeriod`,
// `MaxRoundPeriod`, `RecoveryPeriod`, and
// `ProbabilisticFilteringMaxAllowedErrorRate`. Ported onto
// `@thresh/runtime/placement/repartitioning/activation-repartitioner-options`
// (`ActivationRepartitionerOptions`/`ActivationRepartitionerOptionsValidator`),
// a faithful port with `TimeSpan` fields represented as millisecond numbers
// (`*Ms`), matching this codebase's existing duration convention.
import {
  ActivationRepartitionerConfigurationError,
  ActivationRepartitionerOptionsValidator,
  DEFAULT_ANCHORING_FILTER_ENABLED,
  DEFAULT_MAX_EDGE_COUNT,
  DEFAULT_MAXIMUM_ROUND_PERIOD_MS,
  DEFAULT_MINUMUM_ROUND_PERIOD_MS,
  DEFAULT_PROBABILISTIC_FILTERING_MAX_ALLOWED_ERROR,
  DEFAULT_RECOVERY_PERIOD_MS,
  defaultActivationRepartitionerOptions,
} from "@thresh/runtime/placement/repartitioning/activation-repartitioner-options";
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("UnitTests.ActivationRepartitioningTests.OptionsTests", () => {
  orleansTest(
    "UnitTests.ActivationRepartitioningTests.OptionsTests.ConstantsShouldNotChange",
    () => {
      expect(DEFAULT_ANCHORING_FILTER_ENABLED).toBe(true);
      expect(DEFAULT_PROBABILISTIC_FILTERING_MAX_ALLOWED_ERROR).toBe(0.01);
      expect(DEFAULT_MAX_EDGE_COUNT).toBe(10_000);
      expect(DEFAULT_MINUMUM_ROUND_PERIOD_MS).toBe(60_000); // TimeSpan.FromMinutes(1)
      expect(DEFAULT_MAXIMUM_ROUND_PERIOD_MS).toBe(120_000); // TimeSpan.FromMinutes(2)
      expect(DEFAULT_RECOVERY_PERIOD_MS).toBe(60_000); // TimeSpan.FromMinutes(1)
    },
  );

  const invalidCases: ReadonlyArray<
    readonly [
      maxEdgeCount: number,
      maxUnprocessedEdges: number,
      minRoundPeriodMinutes: number,
      maxRoundPeriodMinutes: number,
      recoveryPeriodMinutes: number,
      probabilisticFilteringMaxAllowedErrorRate: number,
    ]
  > = [
    [0, 1, 1, 1, 1, 0.01],
    [1, 0, 1, 1, 1, 0.01],
    [1, 1, 0, 1, 1, 0.01],
    [1, 1, 1, 0, 1, 0.01],
    [1, 1, 1, 1, -1, 0.01],
    [1, 1, 2, 1, 1, 0.01],
    [1, 1, 2, 1, 2, 0.01],
    [1, 1, 2, 1, 2, 0.1],
  ];

  orleansTest(
    "UnitTests.ActivationRepartitioningTests.OptionsTests.InvalidOptionsShouldThrow",
    () => {
      for (const [
        maxEdgeCount,
        maxUnprocessedEdges,
        minRoundPeriodMinutes,
        maxRoundPeriodMinutes,
        recoveryPeriodMinutes,
        probabilisticFilteringMaxAllowedErrorRate,
      ] of invalidCases) {
        const options = {
          ...defaultActivationRepartitionerOptions(),
          maxEdgeCount,
          minRoundPeriodMs: minRoundPeriodMinutes * 60_000,
          maxRoundPeriodMs: maxRoundPeriodMinutes * 60_000,
          recoveryPeriodMs: recoveryPeriodMinutes * 60_000,
          maxUnprocessedEdges,
          probabilisticFilteringMaxAllowedErrorRate,
        };

        const validator = new ActivationRepartitionerOptionsValidator(options);
        expect(() => validator.validateConfiguration()).toThrow(
          ActivationRepartitionerConfigurationError,
        );
      }
    },
  );
});
