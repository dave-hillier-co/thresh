// Ported from dotnet/orleans test/Orleans.Runtime.Tests/MembershipTests/SilosStopTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("UnitTests.MembershipTests.SilosStopTests", () => {
  // Steers the target grain onto a specific silo via
  // `RequestContext.Set(IPlacementDirector.PlacementHintKey, ...)` (retrying
  // `GetGrain` until the activation lands there) and calls a generic
  // `ILongRunningTaskGrain<bool>` (open generic grain interfaces are
  // unrepresentable here — GAP-GENERIC-GRAINS). Neither the placement-hint
  // steering mechanism this test depends on to reliably target the secondary
  // silo, nor generic grain interfaces, exist in this harness.
  orleansTest.excluded(
    "DI/hosting builders: relies on IPlacementDirector.PlacementHintKey placement steering (no equivalent surface) and a generic ILongRunningTaskGrain<bool> interface (GAP-GENERIC-GRAINS)",
    "UnitTests.MembershipTests.SilosStopTests.SiloUngracefulShutdown_OutstandingRequestsBreak",
  );

  // This file's single upstream Fact is entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
