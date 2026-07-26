// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/MultifacetGrainTest.cs @ v10.1.0 (MIT).
//
// RWReferences ports onto `castGrainReference` (`packages/core/src/grain-reference.ts`),
// the same `AsReference<T>` re-typing feature the sibling
// `grain-reference-cast-tests.test.ts` un-gaps under GAP-GRAIN-REF-CAST.
//
// RWReferencesInvalidCastException is excluded, not ported: upstream casts with
// a bare C# `(IMultifacetWriter)reader` — no `AsReference`/`Cast` call at all —
// which throws `InvalidCastException` synchronously because .NET's codegen
// builds a distinct concrete reference class per interface and the CLR
// validates the cast against that class's actual runtime type. TypeScript has
// no equivalent: a bare `as IMultifacetWriter` is a compile-time-only
// annotation with zero runtime effect, so there is nothing to assert without
// going through the explicit `castGrainReference` API this framework
// provides — and that API validates lazily by design (Orleans parity for
// `AsReference`/`Cast`, exercised by the sibling file's FailSideCast* tests).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { castGrainReference } from "@thresh/core/grain-reference";
import {
  IMultifacetFactoryTestGrain,
  IMultifacetReader,
  IMultifacetTestGrain,
  IMultifacetWriter,
  MultifacetFactoryTestGrain,
  MultifacetTestGrain,
} from "@thresh/parity/grains/impl/multifacet-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

describe("DefaultCluster.Tests.General.MultifacetGrainTest", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        {
          ctor: MultifacetTestGrain,
          interfaces: [IMultifacetTestGrain, IMultifacetReader, IMultifacetWriter],
        },
        { ctor: MultifacetFactoryTestGrain, interfaces: [IMultifacetFactoryTestGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.General.MultifacetGrainTest.RWReferences", async () => {
    const writer = cluster.getGrain(IMultifacetWriter, randomIntegerKey());
    const reader = castGrainReference(writer, IMultifacetReader);

    const x = 1234;
    await writer.setValue(x);
    const y = await reader.getValue();
    expect(y).toBe(x);
  });

  orleansTest.excluded(
    "no runtime interface-cast mechanism in TS outside the explicit castGrainReference API",
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
