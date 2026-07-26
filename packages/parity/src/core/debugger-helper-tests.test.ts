// Ported from dotnet/orleans test/Orleans.Core.Tests/DebuggerHelperTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("DefaultCluster.Tests.General.DebuggerHelperTests", () => {
  orleansTest.excluded(
    ".NET reflection-based OrleansDebuggerHelper / IGrainBase-Cast() debugging aid has no equivalent in this framework",
    "DefaultCluster.Tests.General.DebuggerHelperTests.DebuggerHelper_GetGrainInstance",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
