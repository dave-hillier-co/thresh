// Ported from dotnet/orleans test/Orleans.Core.Tests/Diagnostics/ClientLifecycleEventsTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("UnitTests.Diagnostics.ClientLifecycleEventsTests", () => {
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.ClientLifecycleEventsTests.Lifecycle_EmitsObserverAndStageEvents",
  );
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.ClientLifecycleEventsTests.Lifecycle_OnStartFailure_EmitsObserverFailed",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
