// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/OneWayCallTests.cs @ v10.1.0 (MIT).
//
// Every `[OneWay]` method on upstream `IOneWayGrain` (`Notify`,
// `NotifyValueTask`, `ThrowsOneWay*`, `NotifyOtherGrain*`) takes an
// `ISimpleGrainObserver` client object reference as its only way to observe
// that the fire-and-forget call actually ran: the assertions here hinge on
// the caller's task completing synchronously *and* the callback/side effect
// still landing on the observer. This framework has no client object
// reference mechanism (see observer.test.ts), so there is no way to observe
// completion of the one-way call the way these tests require; the `oneWay`
// invoke option itself (`packages/core/src/invoke-options.ts`) exists but
// every test in this file is blocked on the observer callback, not on
// one-way dispatch.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.General.OneWayCallTests", () => {
  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.OneWayCallTests.OneWayMethodsReturnSynchronously_ViaClient",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.OneWayCallTests.OneWayMethodReturnSynchronously_ViaGrain",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.OneWayCallTests.OneWayMethodsReturnSynchronously_ViaClient_ValueTask",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.OneWayCallTests.OneWayMethodReturnSynchronously_ViaGrain_ValueTask",
  );
});
