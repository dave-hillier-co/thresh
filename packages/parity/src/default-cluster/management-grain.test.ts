// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ManagementGrainTests.cs @ v10.1.0 (MIT).
//
// `GetActivationAddressTest` calls `IManagementGrain.GetActivationAddress`
// (a system-target management grain that queries the catalog for a grain's
// current activation address) and also exercises `[StatelessWorker]`
// placement, which has no single activation address by design. This
// framework has no management grain / silo-control system target
// (GAP-MGMT-GRAIN) to query activation addresses through.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.OrleansRuntime.ManagementGrainTests", () => {
  orleansTest.gap(
    "GAP-MGMT-GRAIN",
    "UnitTests.OrleansRuntime.ManagementGrainTests.GetActivationAddressTest",
  );
});
