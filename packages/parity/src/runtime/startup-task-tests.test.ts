// Ported from dotnet/orleans test/Orleans.Runtime.Tests/StartupTaskTests.cs @ v10.1.0 (MIT).
// Depends on ISiloBuilder.AddStartupTask, which runs IStartupTask instances
// (with grain-factory access) after silo startup but before it accepts
// requests. This framework's silo builder has no startup-task registration
// hook.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.StartupTaskTests", () => {
  orleansTest.gap(
    "GAP-STARTUP-TASK",
    "DefaultCluster.Tests.StartupTaskTests.StartupTaskCanCallGrains",
  );
});
