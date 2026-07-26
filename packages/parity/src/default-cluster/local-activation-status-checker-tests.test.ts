// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/LocalActivationStatusCheckerTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { grainReferenceIdentity } from "@thresh/core/grain-reference";
import type { GrainId } from "@thresh/core/grain-id";
import type { ClientNode } from "@thresh/client/client-node";
import { ISimpleGrain, SimpleGrain } from "@thresh/parity/grains/impl/simple-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";
import { createClusterClient } from "@thresh/parity/support/client";

describe("DefaultCluster.Tests.LocalActivationStatusCheckerTests", () => {
  let cluster: TestCluster;
  let client: ClientNode;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: SimpleGrain, interfaces: [ISimpleGrain] }],
    });
    client = await createClusterClient(cluster, [
      { ctor: SimpleGrain, interfaces: [ISimpleGrain] },
    ]);
  });

  afterAll(async () => {
    await client.close();
    await cluster.dispose();
  });

  const grainIdOf = (ref: unknown): GrainId => grainReferenceIdentity(ref)!.grainId;

  orleansTest(
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ShouldReturnTrueForLocallyActivatedGrain",
    async () => {
      const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      await grain.setA(42);

      const grainId = grainIdOf(grain);
      const activatedOn = cluster.silos.filter((silo) => silo.host.isActive(grainId));
      expect(activatedOn).toHaveLength(1);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ShouldReturnFalseForNonActivatedGrain",
    () => {
      const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      const grainId = grainIdOf(grain);
      expect(cluster.silos.some((silo) => silo.host.isActive(grainId))).toBe(false);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ShouldReturnFalseForDifferentGrainIdentity",
    async () => {
      const grain1 = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      await grain1.setA(42);

      const grain2 = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      const grainId2 = grainIdOf(grain2);
      expect(cluster.silos.some((silo) => silo.host.isActive(grainId2))).toBe(false);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ClientShouldAlwaysReturnFalseForIsLocallyActivated",
    async () => {
      const grain = client.getGrain(ISimpleGrain, randomIntegerKey());
      await grain.setA(42);

      const grainId = grainIdOf(grain);
      expect(client.isActive(grainId)).toBe(false);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ClientShouldReturnFalseForNonActivatedGrain",
    () => {
      const grain = client.getGrain(ISimpleGrain, randomIntegerKey());
      const grainId = grainIdOf(grain);
      expect(client.isActive(grainId)).toBe(false);
    },
  );
});
