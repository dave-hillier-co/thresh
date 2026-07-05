// Ported from dotnet/orleans test/Orleans.Runtime.Tests/GrainActivatorTests.cs @ v10.1.0 (MIT).
// Depends on IGrainActivator / IConfigureGrainTypeComponents: a pluggable,
// per-grain-type hook that lets application code bypass DI-based grain
// instantiation entirely and observe disposal. This framework always
// activates grains through its own fixed construction path with no
// equivalent extensibility point.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.GrainActivatorTests", () => {
  const testClass = "UnitTests.General.GrainActivatorTests";

  orleansTest.gap("GAP-GRAIN-ACTIVATOR", `${testClass}.CanUseCustomGrainActivatorToCreateGrains`);
  orleansTest.gap("GAP-GRAIN-ACTIVATOR", `${testClass}.CanUseCustomGrainActivatorToReleaseGrains`);
});
