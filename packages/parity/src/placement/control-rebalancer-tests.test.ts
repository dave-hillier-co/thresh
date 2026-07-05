// Ported from dotnet/orleans test/Orleans.Placement.Tests/ActivationRebalancingTests/ControlRebalancerTests.cs @ v10.1.0 (MIT).
//
// Exercises `IActivationRebalancer`'s control surface: `GetRebalancingReport`,
// `SuspendRebalancing`/`ResumeRebalancing` (including a timed suspension that
// auto-resumes), and publish-subscribe report listeners
// (`SubscribeToReports`/`UnsubscribeFromReports`/`IActivationRebalancerReportListener`).
// This framework's `ActivationRebalancerWorker` has internal `suspend()` /
// `resume()` / `report()` methods, but `SiloHost` only surfaces the read-only
// `rebalancingReport()` getter: there is no public API to suspend/resume the
// worker, no timed auto-resuming suspension, and no report-listener
// pub/sub mechanism at all (GAP-REBALANCER-CONTROL).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ActivationRebalancingTests.ControlRebalancerTests", () => {
  orleansTest.gap(
    "GAP-REBALANCER-CONTROL",
    "UnitTests.ActivationRebalancingTests.ControlRebalancerTests.Rebalancer_Should_Be_Controllable_And_Report_To_Listeners",
  );
});
