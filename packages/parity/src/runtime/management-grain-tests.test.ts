// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ManagementGrainTests.cs @ v10.1.0 (MIT).
// Every case here queries `IManagementGrain` (GetHosts / GetDetailedHosts /
// GetSimpleGrainStatistics) — the system management grain that surfaces
// cluster topology and per-grain-type activation counts.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { IManagementGrain } from "@tsva/core/management-grain";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { ISimpleGrain, SimpleGrain } from "@tsva/parity/grains/impl/simple-grain";
import { ITestGrain, TestGrain } from "@tsva/parity/grains/impl/test-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

describe("UnitTests.Management.ManagementGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [
        { ctor: SimpleGrain, interfaces: [ISimpleGrain] },
        { ctor: TestGrain, interfaces: [ITestGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("UnitTests.Management.ManagementGrainTests.GetHosts", async () => {
    if (cluster.silos.length === 1) await cluster.startAdditionalSilo();
    const mgmtGrain = cluster.getGrain(IManagementGrain, 0n);
    const siloStatuses = await mgmtGrain.getHosts(true);
    expect(siloStatuses).not.toBeNull();
    expect(siloStatuses.size).toBe(cluster.silos.length);
  });

  orleansTest("UnitTests.Management.ManagementGrainTests.GetDetailedHosts", async () => {
    if (cluster.silos.length === 1) await cluster.startAdditionalSilo();
    const mgmtGrain = cluster.getGrain(IManagementGrain, 0n);
    const siloStatuses = await mgmtGrain.getDetailedHosts(true);
    expect(siloStatuses).not.toBeNull();
    expect(siloStatuses.length).toBe(cluster.silos.length);
  });

  orleansTest("UnitTests.Management.ManagementGrainTests.GetSimpleGrainStatistics", async () => {
    const mgmtGrain = cluster.getGrain(IManagementGrain, 0n);
    // At least one grain must have been activated for there to be anything to report.
    await cluster.getGrain(ISimpleGrain, randomIntegerKey()).getA();
    const stats = await mgmtGrain.getSimpleGrainStatistics(null);
    expect(stats.length).toBeGreaterThan(0);
    for (const s of stats) expect(s.grainType.endsWith("Activation")).toBe(false);
  });

  orleansTest(
    "UnitTests.Management.ManagementGrainTests.GetSimpleGrainStatistics_ActivationCounts",
    async () => {
      await runGetStatisticsTest(
        cluster,
        "UnitTests.Grains.SimpleGrain",
        (key) => cluster.getGrain(ISimpleGrain, key).getA(),
      );
    },
  );

  orleansTest(
    "UnitTests.Management.ManagementGrainTests.GetTestGrainStatistics_ActivationCounts",
    async () => {
      await runGetStatisticsTest(
        cluster,
        "UnitTests.Grains.TestGrain",
        (key) => cluster.getGrain(ITestGrain, key).getKey(),
      );
    },
  );
});

async function runGetStatisticsTest(
  cluster: TestCluster,
  grainType: string,
  callGrainMethod: (key: bigint) => Promise<unknown>,
): Promise<void> {
  const mgmtGrain = cluster.getGrain(IManagementGrain, 0n);
  let stats = await mgmtGrain.getSimpleGrainStatistics(null);
  expect(stats.length).toBeGreaterThan(0);

  const initialActivationsCount = stats
    .filter((s) => s.grainType === grainType)
    .reduce((sum, s) => sum + s.activationCount, 0);

  await callGrainMethod(randomIntegerKey());

  stats = await mgmtGrain.getSimpleGrainStatistics(null);
  const expectedActivationsCount = initialActivationsCount + 1;
  const actualActivationsCount = stats
    .filter((s) => s.grainType === grainType)
    .reduce((sum, s) => sum + s.activationCount, 0);
  expect(actualActivationsCount).toBe(expectedActivationsCount);
}
