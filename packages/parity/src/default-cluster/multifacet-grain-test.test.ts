// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/MultifacetGrainTest.cs @ v10.1.0 (MIT).
//
// RWReferences / RWReferencesInvalidCastException turn on
// `GrainReference.AsReference<T>()` re-typing an already-obtained reference to
// a sibling interface it wasn't fetched through — the same gap the sibling
// `grain-reference-cast-tests.test.ts` file already carries under
// GAP-GRAIN-REF-CAST, so they are gapped under the same tag rather than a new
// one.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  IMultifacetFactoryTestGrain,
  IMultifacetTestGrain,
  MultifacetFactoryTestGrain,
  MultifacetTestGrain,
} from "@tsva/parity/grains/impl/multifacet-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

describe("DefaultCluster.Tests.General.MultifacetGrainTest", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: MultifacetTestGrain, interfaces: [IMultifacetTestGrain] },
        { ctor: MultifacetFactoryTestGrain, interfaces: [IMultifacetFactoryTestGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.General.MultifacetGrainTest.RWReferences",
  );

  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.General.MultifacetGrainTest.RWReferencesInvalidCastException",
  );

  orleansTest("DefaultCluster.Tests.General.MultifacetGrainTest.MultifacetFactory", async () => {
    const factory = cluster.getGrain(IMultifacetFactoryTestGrain, randomIntegerKey());
    const grain = cluster.getGrain(IMultifacetTestGrain, randomIntegerKey());
    const writer = await factory.getWriterOf(grain);
    const reader = await factory.getReaderOf(grain);
    await writer.setValue(5);
    const v = await reader.getValue();
    expect(v).toBe(5);
  });

  orleansTest(
    "DefaultCluster.Tests.General.MultifacetGrainTest.Multifacet_InterfacesAsArguments",
    async () => {
      const factory = cluster.getGrain(IMultifacetFactoryTestGrain, randomIntegerKey());
      const grain = cluster.getGrain(IMultifacetTestGrain, randomIntegerKey());
      await factory.setReader(grain);
      await factory.setWriter(grain);
      const writer = await factory.getWriter();
      const reader = await factory.getReader();
      await writer!.setValue(10);
      const v = await reader!.getValue();
      expect(v).toBe(10);
    },
  );
});
