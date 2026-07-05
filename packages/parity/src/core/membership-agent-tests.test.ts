// Ported from dotnet/orleans test/Orleans.Core.Tests/Membership/MembershipAgentTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("NonSilo.Tests.Membership.MembershipAgentTests", () => {
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipAgentTests.MembershipAgent_LifecycleStages_GracefulShutdown",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipAgentTests.MembershipAgent_LifecycleStages_UngracefulShutdown",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipAgentTests.MembershipAgent_UpdateIAmAlive",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipAgentTests.MembershipAgent_LifecycleStages_ValidateInitialConnectivity_Success",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipAgentTests.MembershipAgent_LifecycleStages_ValidateInitialConnectivity_Failure",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
