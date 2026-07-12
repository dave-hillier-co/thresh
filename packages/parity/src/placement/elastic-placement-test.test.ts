// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/ElasticPlacementTest.cs @ v10.1.0 (MIT).
//
// The four elasticity tests are `[SkippableFact(Skip = "...")]` upstream
// (https://github.com/dotnet/orleans/issues/4008) — skipped there too. The
// remaining two (`LoadAwareGrainShouldNotAttemptToCreateActivationsOn*`) now
// have their headline dependency — an `OverloadDetector`/latchable
// CPU-usage source, and a gateway that sheds load — ported (GAP-LOAD-SHEDDING,
// see `load-shedding-test.test.ts`). What still blocks them is a DIFFERENT
// gap: both route their setup through `GetGrainAtSilo`, upstream's helper for
// pinning a fresh grain onto a specific NEWLY STARTED silo via
// `RequestContext.Set(IPlacementDirector.PlacementHintKey, silo)` — this
// framework has no RequestContext-driven placement hint (GAP-REQUEST-CONTEXT),
// so a test cannot reliably get a reference to the tainted silo's activation
// to latch. Retagged under GAP-LOAD-SHEDDING still, since that's the tag with
// the `todo.md` entry tracking this pair; the placement-hint half of the
// blocker is tracked separately under GAP-REQUEST-CONTEXT.
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
