// Ported from dotnet/orleans test/Orleans.Runtime.Tests/Forwarding/ShutdownSiloTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("Tester.Forwarding.ShutdownSiloTests", () => {
  orleansTest.excluded(
    "skipped upstream (https://github.com/dotnet/orleans/issues/6423)",
    "Tester.Forwarding.ShutdownSiloTests.SiloGracefulShutdown_ForwardPendingRequest",
  );

  // All four Facts locate their target grain via
  // `GetXxxOnSecondary`, steering placement with
  // `RequestContext.Set(IPlacementDirector.PlacementHintKey, ...)` and polling
  // `GetRuntimeInstanceId()` until the activation lands on the chosen silo.
  // That placement-hint steering mechanism has no equivalent in this harness
  // (see `UnitTests.General.OneWayDeactivationTests.OneWay_Deactivation_CacheInvalidated`),
  // and the cluster itself is configured to require Azure Table storage
  // clustering (`UseAzureStorageClustering`) plus `ActivationCountBasedPlacement`
  // wired through DI — external-dependency and DI/hosting-builder machinery
  // this harness does not have either.
  const placementHintReason =
    "DI/hosting builders: relies on IPlacementDirector.PlacementHintKey placement steering (no equivalent surface) plus Azure Table storage clustering and DI-registered placement strategies";

  orleansTest.excluded(
    placementHintReason,
    "Tester.Forwarding.ShutdownSiloTests.SiloGracefulShutdown_PendingRequestTimers",
  );

  orleansTest.excluded(
    placementHintReason,
    "Tester.Forwarding.ShutdownSiloTests.SiloGracefulShutdown_StuckTimers",
  );

  orleansTest.excluded(
    placementHintReason,
    "Tester.Forwarding.ShutdownSiloTests.SiloGracefulShutdown_StuckActivation",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
