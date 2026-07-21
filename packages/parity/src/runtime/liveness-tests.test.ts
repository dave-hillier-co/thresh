// Ported from dotnet/orleans test/Orleans.Runtime.Tests/MembershipTests/LivenessTests.cs @ v10.1.0 (MIT).
// This suite kills/restarts silos mid-test, so (per convention) each case
// builds its own cluster rather than sharing one across the describe block.
//
// `Do_Liveness_OracleTest_2`'s traffic driver (`SendTraffic`) also calls
// `LogGrainIdentity`, which fetches `GetGrainReference`/`GetUniqueId`/
// `GetRuntimeInstanceId` purely to compose a diagnostic log line with no
// assertion attached — dropped along with those grain methods (see
// liveness-test-grain-interfaces.ts). The asserted behaviour
// (`GetPrimaryKeyLong`, `GetLabel`) is preserved exactly.
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { IManagementGrain } from "@tsva/core/management-grain";
import { TestCluster, type TestSiloHandle } from "@tsva/testing/test-cluster";
import {
  ILivenessTestGrain,
  LivenessTestGrain,
} from "@tsva/parity/grains/impl/liveness-test-grain";

const numGrains = 20;

async function sendTraffic(cluster: TestCluster, key: bigint, startTimers = false): Promise<void> {
  const grain = cluster.getGrain(ILivenessTestGrain, key);
  expect(await grain.getPrimaryKeyLong()).toBe(key);
  expect(await grain.getLabel()).toBe(key.toString());
  if (startTimers) await grain.startTimer();
}

async function doLivenessOracleTest2(
  cluster: TestCluster,
  targetSilo: TestSiloHandle,
  restart: boolean,
  startTimers = false,
): Promise<void> {
  for (let i = 0; i < numGrains; i += 1) {
    await sendTraffic(cluster, BigInt(i + 1), startTimers);
  }

  if (restart) {
    await cluster.restartSilo(targetSilo);
  } else {
    await cluster.killSilo(targetSilo);
  }

  for (let i = 0; i < numGrains; i += 1) {
    await sendTraffic(cluster, BigInt(i + 1));
  }
  for (let i = numGrains; i < 2 * numGrains; i += 1) {
    await sendTraffic(cluster, BigInt(i + 1));
  }
}

describe("UnitTests.MembershipTests.LivenessTests_MembershipGrain", () => {
  // Upstream reads back every silo's `SiloStatus` from `IManagementGrain`,
  // stops the target silo, then asserts it now reads back as ShuttingDown,
  // Stopping, or Dead while the others stay Active. This harness's
  // `TestCluster.stopSilo` removes the stopped silo from the membership view
  // outright (`StaticMembershipService.removeSilo`) rather than tombstoning
  // it with a terminal status, so the adapted assertion below checks that the
  // stopped silo simply drops out of `getHosts()` and the survivors stay
  // Active — the same "silo is gone, the rest are unaffected" behaviour,
  // expressed the way this harness's membership model represents it.
  orleansTest(
    "UnitTests.MembershipTests.LivenessTests_MembershipGrain.Liveness_Grain_1",
    async () => {
      const cluster = await TestCluster.start({
        initialSilos: 2,
        grains: [{ ctor: LivenessTestGrain, interfaces: [ILivenessTestGrain] }],
      });
      try {
        const silo3 = await cluster.startAdditionalSilo();
        const mgmt = cluster.getGrain(IManagementGrain, 0n);

        let statuses = await mgmt.getHosts(false);
        expect(statuses.size).toBe(3);
        for (const status of statuses.values()) expect(status).toBe("active");

        await cluster.stopSilo(silo3);

        statuses = await mgmt.getHosts(false);
        expect(statuses.size).toBe(2);
        expect([...statuses.keys()].some((a) => a.equals(silo3.address))).toBe(false);
        for (const status of statuses.values()) expect(status).toBe("active");
      } finally {
        await cluster.dispose();
      }
    },
  );

  orleansTest(
    "UnitTests.MembershipTests.LivenessTests_MembershipGrain.Liveness_Grain_2_Restart_GW",
    async () => {
      const cluster = await TestCluster.start({
        initialSilos: 2,
        grains: [{ ctor: LivenessTestGrain, interfaces: [ILivenessTestGrain] }],
      });
      try {
        await cluster.startAdditionalSilo();
        const target = cluster.silos[1]!;
        await doLivenessOracleTest2(cluster, target, /* restart */ true);
      } finally {
        await cluster.dispose();
      }
    },
  );

  orleansTest(
    "UnitTests.MembershipTests.LivenessTests_MembershipGrain.Liveness_Grain_3_Restart_Silo_1",
    async () => {
      const cluster = await TestCluster.start({
        initialSilos: 2,
        grains: [{ ctor: LivenessTestGrain, interfaces: [ILivenessTestGrain] }],
      });
      try {
        await cluster.startAdditionalSilo();
        const target = cluster.silos[2]!;
        await doLivenessOracleTest2(cluster, target, /* restart */ true);
      } finally {
        await cluster.dispose();
      }
    },
  );

  orleansTest(
    "UnitTests.MembershipTests.LivenessTests_MembershipGrain.Liveness_Grain_4_Kill_Silo_1_With_Timers",
    async () => {
      const cluster = await TestCluster.start({
        initialSilos: 2,
        grains: [{ ctor: LivenessTestGrain, interfaces: [ILivenessTestGrain] }],
      });
      try {
        await cluster.startAdditionalSilo();
        const target = cluster.silos[2]!;
        await doLivenessOracleTest2(cluster, target, /* restart */ false, /* startTimers */ true);
      } finally {
        await cluster.dispose();
      }
    },
  );
});
