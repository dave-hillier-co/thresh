// Ported from dotnet/orleans test/Orleans.Core.Tests/Diagnostics/MembershipEventsTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("UnitTests.Diagnostics.MembershipEventsTests", () => {
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.MembershipEventsTests.EmitChanges_EmitsViewChangedOnly",
  );
  orleansTest.excluded(
    "instrumentation of Orleans' internal membership-table protocol and staged ServiceLifecycle notification pipeline — Kubernetes is the membership authority in this framework by design, and there is no staged lifecycle-notification pipeline to instrument (see docs/deviations.md)",
    "UnitTests.Diagnostics.MembershipEventsTests.MembershipDiagnosticObserver_DerivesStatusTransitions_FromViewChanged",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
