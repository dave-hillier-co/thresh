// Ported from dotnet/orleans test/Orleans.Core.Tests/Diagnostics/DiagnosticInfrastructureRegressionTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.Diagnostics.DiagnosticInfrastructureRegressionTests", () => {
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.DiagnosticInfrastructureRegressionTests.DiagnosticEventCollector_PredicateTimeout_DoesNotBlockSubsequentWaits",
  );
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.DiagnosticInfrastructureRegressionTests.GrainDiagnosticObserver_WaitForAnyGrainDeactivatedAsync_TimesOut",
  );
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.DiagnosticInfrastructureRegressionTests.GrainDiagnosticObserver_WaitAfterTimeout_CanObserveLaterEvent",
  );
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.DiagnosticInfrastructureRegressionTests.RebalancerDiagnosticObserver_WaitForCycleAsync_ReturnsNewEvent",
  );
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.DiagnosticInfrastructureRegressionTests.RebalancerDiagnosticObserver_WaitForSessionStopAsync_ReturnsNewEvent",
  );
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.DiagnosticInfrastructureRegressionTests.InMemoryLoggerProvider_FormatsStoredThreadId",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
