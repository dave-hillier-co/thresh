// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/ExternalTypesTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import {
  ExternalTypeGrain,
  IExternalTypeGrain,
} from "@thresh/parity/grains/impl/external-type-grain";

describe("DefaultCluster.Tests.General.ExternalTypesTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: ExternalTypeGrain, interfaces: [IExternalTypeGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.General.ExternalTypesTests.ExternalTypesTest_GrainWithEnumExternalTypeParam",
    async () => {
      const grainWithEnumTypeParam = cluster.getGrain(IExternalTypeGrain, 0n);
      await grainWithEnumTypeParam.getEnumModel();
    },
  );
});
