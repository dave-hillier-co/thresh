// Ported from dotnet/orleans test/Orleans.Core.Tests/Membership/MembershipTableSnapshotTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("NonSilo.Tests.Membership.MembershipTableSnapshotTests", () => {
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableSnapshotTests.MembershipTableSnapshot_GetSiloStatus_JoiningSilo",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableSnapshotTests.MembershipTableSnapshot_GetSiloStatus_StoppingSilo",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableSnapshotTests.MembershipTableSnapshot_GetSiloStatus_UnknownSilo",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableSnapshotTests.MembershipTableSnapshot_CreateUpdatePreservesIAmAliveTime",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableSnapshotTests.MembershipTableSnapshot_GetSiloStatus_UnknownSilo_KnownSuccessor",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
