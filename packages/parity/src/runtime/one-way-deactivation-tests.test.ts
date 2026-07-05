// Ported from dotnet/orleans test/Orleans.Runtime.Tests/OneWayDeactivationTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTests.General.OneWayDeactivationTests", () => {
  // Upstream injects a custom IGrainDirectoryCache (via
  // services.AddFromExisting<IGrainDirectoryCache, TestDirectoryCache>()) to
  // record every AddOrUpdate/invalidate operation, uses
  // IPlacementDirector.PlacementHintKey to steer activation onto a chosen
  // silo, and reaches into InProcessSiloHandle.SiloHost.Services for the
  // instrumented cache. That is DI/hosting-builder machinery this harness
  // does not expose: TestCluster has no service-injection surface, no
  // placement-hint mechanism, and no instrumented directory-cache hook to
  // assert against.
  orleansTest.excluded(
    "DI/hosting builders: relies on injecting a custom IGrainDirectoryCache via DI, IPlacementDirector.PlacementHintKey placement steering, and direct SiloHost.Services access — no equivalent test-instrumentation surface exists in this harness",
    "UnitTests.General.OneWayDeactivationTests.OneWay_Deactivation_CacheInvalidated",
  );

  // This file's single upstream Fact is entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
