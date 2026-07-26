// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ConcreteStateClassTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { waitFor } from "@thresh/testing/wait";
import {
  ISimplePersistentGrain,
  SimplePersistentGrain,
} from "@thresh/parity/grains/impl/simple-persistent-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

describe("DefaultCluster.Tests.General.StateClassTests", () => {
  let cluster: TestCluster;
  let time: FakeTimeProvider;

  beforeAll(async () => {
    time = new FakeTimeProvider();
    cluster = await TestCluster.start({
      initialSilos: 2,
      time,
      grains: [{ ctor: SimplePersistentGrain, interfaces: [ISimplePersistentGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.General.StateClassTests.StateClassTests_StateClass",
    async () => {
      const grain = cluster.getGrain(ISimplePersistentGrain, randomIntegerKey());
      const originalVersion = await grain.getVersion();
      await grain.setADeactivating(98, true); // deactivate grain after setting A

      // requestDeactivation() just flags the activation stale; it is actually
      // torn down by the next idle-collection sweep (default every 60s here),
      // so fast-forward the fake clock past one sweep interval.
      time.advance(60_001);
      await waitFor(async () => (await grain.getVersion()) !== originalVersion);

      const newVersion = await grain.getVersion(); // get a new version from the new activation
      expect(newVersion).not.toBe(originalVersion);
      const a = await grain.getA();
      expect(a).toBe(98); // value of A survives deactivation and reactivation of the grain
    },
  );
});
