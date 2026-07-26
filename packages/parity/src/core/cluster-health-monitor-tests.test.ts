// Ported from dotnet/orleans test/Orleans.Core.Tests/Membership/ClusterHealthMonitorTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("NonSilo.Tests.Membership.ClusterHealthMonitorTests", () => {
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_BasicScenario",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_MonitorAllStaleSilos",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_NoIndirectProbes",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_ThreeVotesNeededToKill",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_OneVoteNeededToKill",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_SilosWithStaleCreatedOrJoiningState_OneVoteNeededToKill",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_SilosWithStaleCreatedOrJoiningState_TwoVotesNeededToKill",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_SilosWithStaleCreatedOrJoiningState_ThreeVotesNeededToKill",
  );
  orleansTest.excluded(
    "membership-table protocol test — Kubernetes is the membership authority in this framework by design (see docs/deviations.md); there is no elected-TM-style silo health voting or membership-table state machine to test",
    "NonSilo.Tests.Membership.ClusterHealthMonitorTests.ClusterHealthMonitor_SilosWithStaleCreatedOrJoiningState_Disabled",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
