// Ported from dotnet/orleans test/Orleans.Runtime.Tests/Directories/MultipleGrainDirectoriesTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("Tester.Directories.MultipleGrainDirectoriesTests", () => {
  // Upstream steers activations onto a specific silo via
  // `RequestContext.Set(IPlacementDirector.PlacementHintKey, ...)` and exercises
  // a custom `ICustomDirectoryGrain` wired through a custom `IGrainDirectory`
  // registered via DI (`MultipleGrainDirectoriesTests` is itself an abstract
  // base class instantiated by subclasses in other test projects that supply
  // that directory). Neither the placement-hint steering mechanism nor a
  // DI-injected custom grain directory has an equivalent in this harness (see
  // `UnitTests.General.OneWayDeactivationTests.OneWay_Deactivation_CacheInvalidated`
  // for the same gap).
  orleansTest.excluded(
    "DI/hosting builders: relies on IPlacementDirector.PlacementHintKey placement steering and a custom IGrainDirectory injected via DI — no equivalent test-instrumentation surface exists in this harness",
    "Tester.Directories.MultipleGrainDirectoriesTests.PingGrain",
  );

  // This file's single upstream Fact is entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
