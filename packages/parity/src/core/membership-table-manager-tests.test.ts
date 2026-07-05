// Ported from dotnet/orleans test/Orleans.Core.Tests/Membership/MembershipTableManagerTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("NonSilo.Tests.Membership.MembershipTableManagerTests", () => {
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_NewCluster",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_ExistingCluster",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_Restarted",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_Superseded",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_AlreadyDeclaredDead",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_DeclaredDead_AfterJoining",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_TrySuspectOrKill_ButIAmKill",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_TrySuspectOrKill_AlreadyDead",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_TrySuspectOrKill_DeclareDead_SmallCluster",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_TrySuspectOrKill_ClocksNotSynchronized",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_TrySuspectOrKill_DeclareDead_LargerCluster",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.MembershipTableManagerTests.MembershipTableManager_Refresh",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
