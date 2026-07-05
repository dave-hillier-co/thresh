// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/LoadSheddingTest.cs @ v10.1.0 (MIT).
//
// Both tests latch a silo's reported CPU usage above `LoadSheddingOptions`'s
// threshold via `TestHooksEnvironmentStatisticsProvider`/`OverloadDetector`
// and assert the gateway rejects calls with `GatewayTooBusyException`. This
// framework has no overload detector, load-shedding options, or gateway
// rejection path (GAP-LOAD-SHEDDING) — silos always accept requests.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.LoadSheddingTest", () => {
  orleansTest.gap("GAP-LOAD-SHEDDING", "UnitTests.General.LoadSheddingTest.LoadSheddingBasic");

  orleansTest.gap("GAP-LOAD-SHEDDING", "UnitTests.General.LoadSheddingTest.LoadSheddingComplex");
});
