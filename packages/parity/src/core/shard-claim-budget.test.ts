// Ported from dotnet/orleans test/Orleans.Core.Tests/DurableJobs/ShardClaimBudgetTests.cs @ v10.1.0 (MIT).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// Orleans' `LocalDurableJobManager.ComputeClaimBudget` ramps a freshly joined
// silo's per-reconciliation claim budget linearly from `initialBudget` to
// `maxBudget` over `rampUpDuration`, becoming unlimited once ramp-up elapses.
// This framework's claim budget (`claimBudget` in job-model.ts, driven by
// `DurableJobsOptions.claimRampUpBudget`) is a flat per-step ceiling with no
// time dimension at all — no rampUpDuration, initialBudget/maxBudget pair, or
// elapsed-time interpolation. There is also no `DurableJobsOptionsValidator`
// equivalent: `resolveOptions` (local-durable-job-manager.ts) simply defaults
// unset options rather than throwing on invalid ones. Both are missing
// features, not behavioural differences, so every test in this file is a gap.

describe("NonSilo.Tests.DurableJobs.ShardClaimBudgetTests", () => {
  const NS = "NonSilo.Tests.DurableJobs.ShardClaimBudgetTests";

  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ComputeClaimBudget_AtStartup_ReturnsInitialBudget`,
  );
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ComputeClaimBudget_AtMidpoint_ReturnsInterpolatedBudget`,
  );
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ComputeClaimBudget_JustBeforeEnd_ReturnsNearMaxBudget`,
  );
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ComputeClaimBudget_AfterRampUp_ReturnsUnlimited`,
  );
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ComputeClaimBudget_WellPastRampUp_ReturnsUnlimited`,
  );
  orleansTest.gap("GAP-CLAIM-BUDGET-RAMPUP", `${NS}.ComputeClaimBudget_Disabled_ReturnsUnlimited`);
  orleansTest.gap("GAP-CLAIM-BUDGET-RAMPUP", `${NS}.ComputeClaimBudget_SubtractsPreviousClaims`);
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ComputeClaimBudget_ClaimsExceedBudget_ReturnsZero`,
  );
  orleansTest.gap("GAP-CLAIM-BUDGET-RAMPUP", `${NS}.ComputeClaimBudget_LinearProgressionOverTime`);
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ValidateConfiguration_NegativeShardClaimInitialBudget_Throws`,
  );
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ValidateConfiguration_MaxBudgetLessThanInitial_Throws`,
  );
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ValidateConfiguration_NegativeRampUpDuration_Throws`,
  );
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ValidateConfiguration_ValidShardClaimOptions_DoesNotThrow`,
  );
  orleansTest.gap(
    "GAP-CLAIM-BUDGET-RAMPUP",
    `${NS}.ValidateConfiguration_ZeroRampUpDuration_DoesNotThrow`,
  );
});
