// Ported from dotnet/orleans test/Orleans.Placement.Tests/General/LoadSheddingTest.cs @ v10.1.0 (MIT).
//
// Both tests latch a silo's reported CPU usage above `LoadSheddingOptions`'s
// threshold via `TestHooksEnvironmentStatisticsProvider`/`OverloadDetector`
// and assert the gateway rejects calls with `GatewayTooBusyException`.
// Upstream reaches those two collaborators through the silo's DI
// `IServiceProvider`; this framework's equivalent is `SiloHost.loadShedding`
// (`ClusterNode.siloTestHooks()`), which latches/unlatches the same
// `TestHooksEnvironmentStatisticsProvider` and toggles the same
// `OverloadDetector` this silo's gateway path (`ClusterNode.receiveRequest`)
// consults before dispatching a client-originated request.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { GatewayTooBusyException } from "@thresh/core/errors";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import type { ClientNode } from "@thresh/client/client-node";
import { SimpleGrain } from "@thresh/parity/grains/impl/simple-grain";
import { ISimpleGrain } from "@thresh/parity/grains/interfaces/simple-grain-interfaces";
import { createClusterClient } from "@thresh/parity/support/client";
import { randomIntegerKey } from "@thresh/parity/support/keys";

const CPU_THRESHOLD = 98;

describe("UnitTests.General.LoadSheddingTest", () => {
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 1,
      grains: [{ ctor: SimpleGrain, interfaces: [ISimpleGrain] }],
      loadShedding: { loadSheddingEnabled: true, cpuThreshold: CPU_THRESHOLD },
    });
    client = await createClusterClient(cluster, [{ ctor: SimpleGrain, interfaces: [ISimpleGrain] }]);
  });

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
  });

  function latchIsOverloaded(isOverloaded: boolean): void {
    cluster.primary.host.loadShedding.latchCpuUsage(isOverloaded ? CPU_THRESHOLD + 1 : CPU_THRESHOLD - 1);
  }

  function unlatchIsOverloaded(): void {
    cluster.primary.host.loadShedding.unlatchCpuUsage();
  }

  orleansTest("UnitTests.General.LoadSheddingTest.LoadSheddingBasic", async () => {
    try {
      const grainRef = client.getGrain(ISimpleGrain, randomIntegerKey());
      latchIsOverloaded(true);

      // Do not accept message in overloaded state
      await expect(grainRef.setA(5)).rejects.toThrow(GatewayTooBusyException);
    } finally {
      unlatchIsOverloaded();
    }
  });

  orleansTest("UnitTests.General.LoadSheddingTest.LoadSheddingComplex", async () => {
    try {
      const grainRef = client.getGrain(ISimpleGrain, randomIntegerKey());

      latchIsOverloaded(false);

      await grainRef.setA(1);

      latchIsOverloaded(true);

      // Do not accept message in overloaded state
      await expect(grainRef.setA(2)).rejects.toThrow(GatewayTooBusyException);

      latchIsOverloaded(false);

      // Simple request after overload is cleared should succeed
      await grainRef.setA(4);
    } finally {
      unlatchIsOverloaded();
    }
  });
});
