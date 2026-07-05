// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ObserverTests.cs @ v10.1.0 (MIT).
//
// Every test in this file turns on `IGrainFactory.CreateObjectReference<T>` /
// `DeleteObjectReference<T>`: registering a plain client-side object as a
// weakly-referenced callback target a grain can invoke through a normal grain
// interface (`ISimpleGrainObserver`). This framework has no client object
// reference mechanism at all — grains can only be reached through
// `TestCluster.getGrain`, and there is no equivalent of a callback-only
// reference a grain could hold and invoke back into the caller. Introducing
// one is a real feature addition, so every case here is a gap rather than an
// exclusion.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.General.ObserverTests", () => {
  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_SimpleNotification",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_SimpleNotification_GeneratedFactory",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_DoubleSubscriptionSameReference",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_SubscribeUnsubscribe",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_Unsubscribe",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_DoubleSubscriptionDifferentReferences",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_DeleteObject",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_SubscriberMustBeGrainReference",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.General.ObserverTests.ObserverTest_CreateObjectReference_ThrowsForInvalidArgumentTypes",
  );
});
