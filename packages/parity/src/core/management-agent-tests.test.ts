// Ported from dotnet/orleans test/Orleans.Core.Tests/ManagementAgentTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.ManagementAgentTests", () => {
  orleansTest.excluded(
    "tests .NET enum equality/operator (==, !=, GetHashCode) semantics for SystemStatus, a type this framework does not expose",
    "UnitTests.ManagementAgentTests.SystemStatusEquals",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
