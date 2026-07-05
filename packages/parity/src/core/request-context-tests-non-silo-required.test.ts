// Ported from dotnet/orleans test/Orleans.Core.Tests/General/RequestContextTestsNonSiloRequired.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// This framework has no RequestContext ambient-dictionary concept, no Message class with a
// RequestContextData export/import round-trip through a DeepCopier, and no .NET
// Activity/ActivityId-based CALL_CHAIN_REENTRANCY_HEADER propagation across threads. Every
// method below is specific to that .NET AsyncLocal + Activity + Message-serialization machinery.
describe("UnitTests.General.RequestContextTests_Local", () => {
  orleansTest.excluded(
    ".NET-specific: exercises RequestContext.Export/Import through a Message + DeepCopier under multi-threaded Task.Run contention; no ambient RequestContext or Message export API exists in this framework",
    "UnitTests.General.RequestContextTests_Local.RequestContext_MultiThreads_ExportToMessage",
  );

  orleansTest.excluded(
    ".NET-specific: exercises .NET Activity/ActivityId propagation into RequestContext.CALL_CHAIN_REENTRANCY_HEADER via Message export; no Activity tracing or ambient RequestContext exists in this framework",
    "UnitTests.General.RequestContextTests_Local.RequestContext_ActivityId_ExportToMessage",
  );

  orleansTest.excluded(
    ".NET-specific: exercises .NET Activity/ActivityId round-trip through RequestContext export/import; no Activity tracing or ambient RequestContext exists in this framework",
    "UnitTests.General.RequestContextTests_Local.RequestContext_ActivityId_ExportImport",
  );

  orleansTest.excluded(
    ".NET-specific: relies on RequestContext being backed by AsyncLocal and propagated across Task.Run continuations on the .NET thread pool; no ambient RequestContext exists in this framework",
    "UnitTests.General.RequestContextTests_Local.RequestContext_CrossThread",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
