// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ManagementGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { grainAddressEquals } from "@thresh/core/grain-address";
import { Guid } from "@thresh/core/guid";
import { IManagementGrain } from "@thresh/core/management-grain";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  DumbGrain,
  DumbWorker,
  IDumbGrain,
  IDumbWorker,
} from "@thresh/parity/grains/impl/management-grain-support";

describe("UnitTests.OrleansRuntime.ManagementGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      grains: [
        { ctor: DumbGrain, interfaces: [IDumbGrain] },
        { ctor: DumbWorker, interfaces: [IDumbWorker] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "UnitTests.OrleansRuntime.ManagementGrainTests.GetActivationAddressTest",
    async () => {
      const managementGrain = cluster.getGrain(IManagementGrain, 0n);
      const grain1 = cluster.getGrain(IDumbGrain, Guid.newGuid());
      const grain2 = cluster.getGrain(IDumbGrain, Guid.newGuid());

      let grain1Address = await managementGrain.getActivationAddress(grain1);
      expect(grain1Address).toBeNull();

      await grain1.doNothing();
      grain1Address = await managementGrain.getActivationAddress(grain1);
      expect(grain1Address).not.toBeNull();
      const grain2Address = await managementGrain.getActivationAddress(grain2);
      expect(grain2Address).toBeNull();

      await grain2.doNothing();
      const grain1Address2 = await managementGrain.getActivationAddress(grain1);
      expect(grain1Address2).not.toBeNull();
      expect(grainAddressEquals(grain1Address2!, grain1Address!)).toBe(true);

      const worker = cluster.getGrain(IDumbWorker, 0n);
      await expect(managementGrain.getActivationAddress(worker)).rejects.toThrow();
    },
  );
});
