// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ClientAddressableTests.cs @ v10.1.0 (MIT).
//
// Client-addressable objects are the same underlying mechanism as grain
// observers: a plain client-side object registered via
// `IGrainFactory.CreateObjectReference<T>` so a grain can call back into it
// through an arbitrary interface (not just the observer callback shape). This
// framework has no client object reference mechanism at all (see
// observer.test.ts), so every case here is the same gap.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.ClientAddressableTests", () => {
  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.ClientAddressableTests.TestClientAddressableHappyPath",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.ClientAddressableTests.TestClientAddressableSadPath",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.ClientAddressableTests.GrainShouldSuccessfullyPullFromClientObject",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.ClientAddressableTests.MicroClientAddressableSerialStressTest",
  );

  orleansTest.gap(
    "GAP-OBSERVERS",
    "DefaultCluster.Tests.ClientAddressableTests.MicroClientAddressableParallelStressTest",
  );
});
