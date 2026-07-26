// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/DeactivationTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import {
  ISimplePersistentGrain,
  SimplePersistentGrain,
} from "@thresh/parity/grains/impl/simple-persistent-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

describe("DefaultCluster.Tests.General.DeactivationTests", () => {
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
    "DefaultCluster.Tests.General.DeactivationTests.DeactivateReactivateTiming",
    async () => {
      const x = randomIntegerKey();
      const grain = cluster.getGrain(ISimplePersistentGrain, x);
      const originalVersion = await grain.getVersion();

      const start = Date.now();

      await grain.setADeactivating(99, true); // deactivate grain after setting A value
      // No real deadline is waited on: the fake clock is fast-forwarded past the
      // idle-collection sweep interval so the requested deactivation runs, then
      // a fresh call reactivates a new incarnation, exactly like upstream's
      // "deactivate, then a call gets a new version" sequence.
      time.advance(60_000);
      await new Promise((r) => setTimeout(r, 0));

      const newVersion = await grain.getVersion(); // get a new version from the new activation
      expect(newVersion).not.toBe(originalVersion);

      const elapsedMs = Date.now() - start;
      expect(elapsedMs).toBeLessThan(1000);

      const a = await grain.getA();
      expect(a).toBe(99); // value of A survives deactivation and reactivation of the grain
    },
  );
});
