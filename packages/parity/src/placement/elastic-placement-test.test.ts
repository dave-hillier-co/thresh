// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/ElasticPlacementTest.cs @ v10.1.0 (MIT).
//
// The four elasticity tests are `[SkippableFact(Skip = "...")]` upstream
// (https://github.com/dotnet/orleans/issues/4008) — skipped there too. The
// remaining two (`LoadAwareGrainShouldNotAttemptToCreateActivationsOn*`) now
// have their headline dependency — an `OverloadDetector`/latchable
// CPU-usage source, and a gateway that sheds load — ported (GAP-LOAD-SHEDDING,
// see `load-shedding-test.test.ts`). Their setup routes through
// `GetGrainAtSilo`, upstream's helper for pinning a fresh grain onto a
// specific NEWLY STARTED silo via
// `RequestContext.Set(IPlacementDirector.PlacementHintKey, silo)` — that
// RequestContext-driven placement hint now exists (GAP-REQUEST-CONTEXT
// closed; see `@tsva/runtime/placement/placement-hint`), so `GetGrainAtSilo`
// itself is no longer the blocker. What still blocks these two is that this
// framework's load-aware placement strategy never consults the
// overload/CPU-latch state at all: it is only wired into the GATEWAY's
// load-shedding rejection path (`ClusterNode.receiveRequest` /
// `GatewayTooBusyException`), not into `OnAddActivation`'s candidate
// selection — so an overloaded/busy silo is never excluded as a placement
// candidate. Closing these also needs `IActivationCountBasedPlacementTestGrain`
// (load-aware placement test grain) and its
// `LatchOverloaded`/`UnlatchOverloaded`/`LatchCpuUsage`/`UnlatchCpuUsage`/
// `GetLocation` members, none of which are ported yet. Still tagged
// GAP-LOAD-SHEDDING, the tag with the `todo.md` entry tracking this pair.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.ElasticPlacementTests", () => {
  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_CatchingUp",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_StoppingSilos",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_AllSilosCPUTooHigh",
  );

  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/4008)",
    "UnitTests.General.ElasticPlacementTests.ElasticityTest_AllSilosOverloaded",
  );

  orleansTest.gap(
    "GAP-LOAD-SHEDDING",
    "UnitTests.General.ElasticPlacementTests.LoadAwareGrainShouldNotAttemptToCreateActivationsOnOverloadedSilo",
  );

  orleansTest.gap(
    "GAP-LOAD-SHEDDING",
    "UnitTests.General.ElasticPlacementTests.LoadAwareGrainShouldNotAttemptToCreateActivationsOnBusySilos",
  );
});
