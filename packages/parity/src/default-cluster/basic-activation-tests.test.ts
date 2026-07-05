// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/BasicActivationTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { Guid } from "@tsva/core/guid";
import { ITestGrain, TestGrain } from "@tsva/parity/grains/impl/test-grain";
import { IGuidTestGrain, GuidTestGrain } from "@tsva/parity/grains/impl/guid-test-grain";

// See bugsFound: an activation whose `onActivate` throws surfaces the generic
// "activation unavailable: <id>" GrainCallError to every caller — the real
// application error (message and type) thrown from `onActivate` is discarded,
// so a test that needs to observe *which* error activation failed with cannot
// pass. See packages/runtime/src/activation.ts `beginActivate`, which schedules
// `onActivate` fire-and-forget and only records failure as a boolean
// (`this.state = "invalid"`) — the thrown error itself is swallowed by the
// `.catch(() => {...})` and never stored anywhere `invoke()` can surface it.

// A second, more severe defect: once an activation's `onActivate` fails, a
// *second* call to the same grain identity on a cluster with more than one
// silo does not fail fast — it runs away allocating fresh activations and
// directory registrations without bound, exhausting the process heap (OOM)
// rather than surfacing a rejection. Repro: two silos, a grain whose
// `onActivate` always throws, two sequential calls to the same grain
// reference — the second call never settles. See bugsFound for the isolated
// repro. `BasicActivation_BurstFail` (10,000 concurrent calls to such a grain)
// would trigger exactly this, so its body is written for documentation but
// never executed (gapped, not ported) to avoid hanging this suite.

// RequestContext (Orleans' ambient call-context bag) is not exposed to grain
// implementations anywhere in @tsva/core's public surface — the ambient store
// exists only inside @tsva/runtime (an internal package parity grains do not
// depend on), so a grain cannot read/write it the way `TestRequestContext`
// requires.

describe("DefaultCluster.Tests.General.BasicActivationTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: TestGrain, interfaces: [ITestGrain] },
        { ctor: GuidTestGrain, interfaces: [IGuidTestGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_ActivateAndUpdate",
    async () => {
      const g1Key = BigInt(Math.floor(Math.random() * 1_000_000_000));
      const g2Key = BigInt(Math.floor(Math.random() * 1_000_000_000)) + 1_000_000_000n;
      const g1 = cluster.getGrain(ITestGrain, g1Key);
      const g2 = cluster.getGrain(ITestGrain, g2Key);

      expect(await g1.getKey()).toBe(g1Key);
      expect(await g1.getLabel()).toBe(g1Key.toString());
      expect(await g2.getKey()).toBe(g2Key);
      expect(await g2.getLabel()).toBe(g2Key.toString());

      await g1.setLabel("one");
      expect(await g1.getLabel()).toBe("one");
      expect(await g2.getLabel()).toBe(g2Key.toString());

      const g1a = cluster.getGrain(ITestGrain, g1Key);
      expect(await g1a.getLabel()).toBe("one");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_Guid_ActivateAndUpdate",
    async () => {
      const guid1 = Guid.newGuid();
      const guid2 = Guid.newGuid();

      const g1 = cluster.getGrain(IGuidTestGrain, guid1);
      const g2 = cluster.getGrain(IGuidTestGrain, guid2);
      expect((await g1.getKey()).equals(guid1)).toBe(true);
      expect(await g1.getLabel()).toBe(guid1.toString());
      expect((await g2.getKey()).equals(guid2)).toBe(true);
      expect(await g2.getLabel()).toBe(guid2.toString());

      await g1.setLabel("one");
      expect(await g1.getLabel()).toBe("one");
      expect(await g2.getLabel()).toBe(guid2.toString());

      const g1a = cluster.getGrain(IGuidTestGrain, guid1);
      expect(await g1a.getLabel()).toBe("one");
    },
  );

  orleansTest.gap(
    "GAP-BUG-ACTIVATION-FAILURE-MESSAGE",
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_Fail",
    async () => {
      // Key values of -2 are not allowed in this case.
      let failed = false;
      let key: bigint | undefined;
      try {
        const fail = cluster.getGrain(ITestGrain, -2n);
        key = await fail.getKey();
      } catch (exc) {
        failed = true;
        expect((exc as Error).message).toBe("Primary key cannot be -2 for this test case");
      }
      if (!failed) expect.fail("Should have failed, but instead returned " + String(key));
    },
  );

  orleansTest.gap(
    "GAP-BUG-ACTIVATION-RETRY-STORM",
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_BurstFail",
    async () => {
      let failed = false;
      const key: bigint | undefined = undefined;
      const tasks: Promise<unknown>[] = [];
      try {
        const fail = cluster.getGrain(ITestGrain, -2n);
        for (let i = 0; i < 10_000; i += 1) tasks.push(fail.getKey());
        await Promise.all(tasks);
      } catch {
        failed = true;
        for (const t of tasks) {
          await expect(t).rejects.toThrow("Primary key cannot be -2 for this test case");
        }
      }
      if (!failed) expect.fail("Should have failed, but instead returned " + String(key));
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_ULong_MaxValue",
    async () => {
      const key1 = -1n; // ulong.MaxValue cast to long
      const g1 = cluster.getGrain(ITestGrain, key1);
      expect(await g1.getKey()).toBe(key1);
      expect(await g1.getLabel()).toBe(key1.toString());

      await g1.setLabel("MaxValue");
      expect(await g1.getLabel()).toBe("MaxValue");

      const g1a = cluster.getGrain(ITestGrain, key1);
      expect(await g1a.getLabel()).toBe("MaxValue");
      expect(await g1a.getKey()).toBe(key1);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_ULong_MinValue",
    async () => {
      const key1 = 0n;
      const g1 = cluster.getGrain(ITestGrain, key1);
      expect(await g1.getKey()).toBe(key1);
      expect(await g1.getLabel()).toBe(key1.toString());

      await g1.setLabel("MinValue");
      expect(await g1.getLabel()).toBe("MinValue");

      const g1a = cluster.getGrain(ITestGrain, key1);
      expect(await g1a.getLabel()).toBe("MinValue");
      expect(await g1a.getKey()).toBe(key1);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_Long_MaxValue",
    async () => {
      const key1 = 2147483647n; // int.MaxValue
      const g1 = cluster.getGrain(ITestGrain, key1);
      expect(await g1.getKey()).toBe(key1);
      expect(await g1.getLabel()).toBe(key1.toString());

      await g1.setLabel("MaxValue");
      expect(await g1.getLabel()).toBe("MaxValue");

      const g1a = cluster.getGrain(ITestGrain, key1);
      expect(await g1a.getLabel()).toBe("MaxValue");
      expect(await g1a.getKey()).toBe(key1);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_Long_MinValue",
    async () => {
      const key1 = -9223372036854775808n; // long.MinValue
      const g1 = cluster.getGrain(ITestGrain, key1);
      expect(await g1.getKey()).toBe(key1);
      expect(await g1.getLabel()).toBe(key1.toString());

      await g1.setLabel("MinValue");
      expect(await g1.getLabel()).toBe("MinValue");

      const g1a = cluster.getGrain(ITestGrain, key1);
      expect(await g1a.getLabel()).toBe("MinValue");
      expect(await g1a.getKey()).toBe(key1);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_MultipleGrainInterfaces",
    async () => {
      const simple = cluster.getGrain(
        ITestGrain,
        BigInt(Math.floor(Math.random() * 1_000_000_000)),
      );
      await simple.getMultipleGrainInterfaces_List();
      await simple.getMultipleGrainInterfaces_Array();
    },
  );

  orleansTest.excluded(
    ".NET-specific message-expiry/response-timeout mechanism (OutsideRuntimeClient.SetResponseTimeout) plus a real 10-second wait to let queued messages expire in the silo; no equivalent per-call response-timeout override or expired-message queue exists in this framework",
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_Reentrant_RecoveryAfterExpiredMessage",
  );

  orleansTest.gap(
    "GAP-REQUEST-CONTEXT",
    "DefaultCluster.Tests.General.BasicActivationTests.BasicActivation_TestRequestContext",
  );
});
