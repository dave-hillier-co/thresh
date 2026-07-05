// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ManagementGrainTests.cs @ v10.1.0 (MIT).
// Every case here queries `IManagementGrain` (GetHosts / GetDetailedHosts /
// GetSimpleGrainStatistics) — the system management grain that surfaces
// cluster topology and per-grain-type activation counts. No management grain
// / silo-control system target exists in this harness (GAP-MGMT-GRAIN, the
// same tag used throughout this suite for IManagementGrain-dependent tests).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.Management.ManagementGrainTests", () => {
  orleansTest.gap("GAP-MGMT-GRAIN", "UnitTests.Management.ManagementGrainTests.GetHosts");
  orleansTest.gap("GAP-MGMT-GRAIN", "UnitTests.Management.ManagementGrainTests.GetDetailedHosts");
  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "UnitTests.Management.ManagementGrainTests.GetSimpleGrainStatistics",
  );
  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "UnitTests.Management.ManagementGrainTests.GetSimpleGrainStatistics_ActivationCounts",
  );
  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "UnitTests.Management.ManagementGrainTests.GetTestGrainStatistics_ActivationCounts",
  );
});
