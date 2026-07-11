// Ported from dotnet/orleans test/Orleans.Core.Tests/DurableJobs/ShardClaimBudgetTests.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { durationToMs } from "@tsva/core/duration";
import type { DurableJobsOptions } from "@tsva/core/durable-job";
import {
  computeClaimBudget,
  DurableJobsOptionsValidator,
  UNLIMITED_CLAIM_BUDGET,
} from "@tsva/durable-jobs/local-durable-job-manager";

// Orleans' `LocalDurableJobManager.ComputeClaimBudget` ramps a freshly joined
// silo's per-reconciliation claim budget linearly from `initialBudget` to
// `maxBudget` over `rampUpDuration`, becoming unlimited once ramp-up elapses.
describe("NonSilo.Tests.DurableJobs.ShardClaimBudgetTests", () => {
  const rampUpDurationMs = durationToMs({ minutes: 5 });
  const initialBudget = 2;
  const maxBudget = 20;

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_AtStartup_ReturnsInitialBudget",
    () => {
      const budget = computeClaimBudget(rampUpDurationMs, initialBudget, maxBudget, 0, 0);
      expect(budget).toBe(initialBudget);
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_AtMidpoint_ReturnsInterpolatedBudget",
    () => {
      const midpoint = rampUpDurationMs / 2;
      const budget = computeClaimBudget(rampUpDurationMs, initialBudget, maxBudget, midpoint, 0);
      // At midpoint: 2 + (int)(0.5 * (20 - 2)) = 2 + 9 = 11
      expect(budget).toBe(11);
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_JustBeforeEnd_ReturnsNearMaxBudget",
    () => {
      const nearEnd = rampUpDurationMs - 1;
      const budget = computeClaimBudget(rampUpDurationMs, initialBudget, maxBudget, nearEnd, 0);
      // Should be very close to maxBudget but computed via truncation
      expect(budget).toBeGreaterThanOrEqual(maxBudget - 1);
      expect(budget).toBeLessThanOrEqual(maxBudget);
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_AfterRampUp_ReturnsUnlimited",
    () => {
      const budget = computeClaimBudget(
        rampUpDurationMs,
        initialBudget,
        maxBudget,
        rampUpDurationMs,
        0,
      );
      expect(budget).toBe(UNLIMITED_CLAIM_BUDGET);
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_WellPastRampUp_ReturnsUnlimited",
    () => {
      const budget = computeClaimBudget(
        rampUpDurationMs,
        initialBudget,
        maxBudget,
        durationToMs({ hours: 1 }),
        0,
      );
      expect(budget).toBe(UNLIMITED_CLAIM_BUDGET);
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_Disabled_ReturnsUnlimited",
    () => {
      const budget = computeClaimBudget(0, initialBudget, maxBudget, 0, 0);
      expect(budget).toBe(UNLIMITED_CLAIM_BUDGET);
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_SubtractsPreviousClaims",
    () => {
      const budget = computeClaimBudget(rampUpDurationMs, initialBudget, maxBudget, 0, 1);
      // 2 - 1 = 1
      expect(budget).toBe(1);
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_ClaimsExceedBudget_ReturnsZero",
    () => {
      const budget = computeClaimBudget(rampUpDurationMs, initialBudget, maxBudget, 0, 10);
      expect(budget).toBe(0);
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ComputeClaimBudget_LinearProgressionOverTime",
    () => {
      let previousBudget = 0;
      for (let i = 0; i <= 10; i++) {
        const fraction = i / 10;
        const elapsed = Math.trunc(rampUpDurationMs * fraction);

        const budget = computeClaimBudget(rampUpDurationMs, initialBudget, maxBudget, elapsed, 0);

        if (budget < UNLIMITED_CLAIM_BUDGET) {
          expect(budget).toBeGreaterThanOrEqual(previousBudget);
          previousBudget = budget;
        }
      }
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ValidateConfiguration_NegativeShardClaimInitialBudget_Throws",
    () => {
      const options: DurableJobsOptions = { shardClaimInitialBudget: -1 };
      const validator = new DurableJobsOptionsValidator(options);
      expect(() => validator.validateConfiguration()).toThrow();
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ValidateConfiguration_MaxBudgetLessThanInitial_Throws",
    () => {
      const options: DurableJobsOptions = {
        shardClaimInitialBudget: 10,
        shardClaimMaxBudget: 5,
      };
      const validator = new DurableJobsOptionsValidator(options);
      expect(() => validator.validateConfiguration()).toThrow();
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ValidateConfiguration_NegativeRampUpDuration_Throws",
    () => {
      const options: DurableJobsOptions = { shardClaimRampUpDuration: { seconds: -1 } };
      const validator = new DurableJobsOptionsValidator(options);
      expect(() => validator.validateConfiguration()).toThrow();
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ValidateConfiguration_ValidShardClaimOptions_DoesNotThrow",
    () => {
      const options: DurableJobsOptions = {
        shardClaimInitialBudget: 2,
        shardClaimMaxBudget: 20,
        shardClaimRampUpDuration: { minutes: 5 },
      };
      const validator = new DurableJobsOptionsValidator(options);
      expect(() => validator.validateConfiguration()).not.toThrow();
    },
  );

  orleansTest(
    "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests.ValidateConfiguration_ZeroRampUpDuration_DoesNotThrow",
    () => {
      const options: DurableJobsOptions = { shardClaimRampUpDuration: { ms: 0 } };
      const validator = new DurableJobsOptionsValidator(options);
      expect(() => validator.validateConfiguration()).not.toThrow();
    },
  );
});
