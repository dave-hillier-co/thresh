// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/GrainReferenceCastTests.cs @ v10.1.0 (MIT).
//
// Orleans' `GrainReference.AsReference<T>()`/`IInternalGrainFactory.Cast` cast
// an already-obtained grain reference to a *different* grain interface at
// runtime, same grain id — validated eagerly only against "is this even a
// registered grain interface", and otherwise left to fail lazily the first
// time a call reaches a method the concrete grain type doesn't have. This is
// ported onto `castGrainReference` (`packages/core/src/grain-reference.ts`),
// backed by the `GRAIN_REF_CAST` symbol every grain-factory proxy answers
// (`packages/runtime/src/grain-factory.ts`). TS has no runtime
// `IsAssignableFrom`/`GetType()`, so casts are asserted behaviourally: same
// grain id via `grainReferenceIdentity`, and successful/failing calls.
import { beforeAll, afterAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { castGrainReference, grainReferenceIdentity } from "@thresh/core/grain-reference";
import { GrainCallError } from "@thresh/core/errors";
import { ISimpleGrain, SimpleGrain } from "@thresh/parity/grains/impl/simple-grain";
import {
  IMultifacetReader,
  IMultifacetWriter,
  MultifacetTestGrain,
} from "@thresh/parity/grains/impl/multifacet-grain";
import {
  GeneratorTestDerivedDerivedGrain,
  GeneratorTestDerivedGrain1,
  GeneratorTestDerivedGrain2,
  IGeneratorTestDerivedDerivedGrain,
  IGeneratorTestDerivedGrain1,
  IGeneratorTestDerivedGrain2,
  IGeneratorTestGrain,
} from "@thresh/parity/grains/impl/generator-test-grain";
import { randomIntegerKey } from "@thresh/parity/support/keys";

describe("DefaultCluster.Tests.GrainReferenceCastTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: SimpleGrain, interfaces: [ISimpleGrain] },
        {
          ctor: MultifacetTestGrain,
          interfaces: [IMultifacetReader, IMultifacetWriter],
        },
        { ctor: GeneratorTestDerivedGrain1, interfaces: [IGeneratorTestDerivedGrain1] },
        { ctor: GeneratorTestDerivedGrain2, interfaces: [IGeneratorTestDerivedGrain2] },
        {
          ctor: GeneratorTestDerivedDerivedGrain,
          interfaces: [IGeneratorTestDerivedDerivedGrain],
        },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest("DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefCastFromMyType", () => {
    const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
    const cast = castGrainReference(grain, ISimpleGrain);
    // Casting to the identical interface is a no-op: the very same reference comes back.
    expect(cast).toBe(grain);
    expect(grainReferenceIdentity(cast)).toEqual(grainReferenceIdentity(grain));
  });

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefCastFromMyTypePolymorphic",
    () => {
      const grain = cluster.getGrain(IMultifacetWriter, randomIntegerKey());
      const asReader = castGrainReference(grain, IMultifacetReader);
      expect(grainReferenceIdentity(asReader)?.grainId).toEqual(
        grainReferenceIdentity(grain)?.grainId,
      );
      expect(grainReferenceIdentity(asReader)?.interfaceId).toBe(IMultifacetReader.id);

      const asWriter = castGrainReference(asReader, IMultifacetWriter);
      expect(grainReferenceIdentity(asWriter)?.grainId).toEqual(
        grainReferenceIdentity(grain)?.grainId,
      );
      expect(grainReferenceIdentity(asWriter)?.interfaceId).toBe(IMultifacetWriter.id);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.CastMultifacetRWReference",
    async () => {
      const newValue = 3;
      const writer = cluster.getGrain(IMultifacetWriter, randomIntegerKey());
      const reader = castGrainReference(writer, IMultifacetReader);

      await writer.setValue(newValue);
      const result = await reader.getValue();

      expect(result).toBe(newValue);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.CastMultifacetRWReferenceWaitForResolve",
    async () => {
      const newValue = 4;
      const writer = cluster.getGrain(IMultifacetWriter, randomIntegerKey());
      const reader = castGrainReference(writer, IMultifacetReader);

      await writer.setValue(newValue);
      const result = await reader.getValue();

      expect(result).toBe(newValue);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.CastFailInternalCastFromBadType",
    () => {
      const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      // Casting to something that is not even a registered grain interface
      // (Orleans' `Cast(grain, typeof(bool))`) fails synchronously.
      expect(() => castGrainReference(grain, { id: -1 } as never)).toThrow(TypeError);
    },
  );

  orleansTest("DefaultCluster.Tests.GrainReferenceCastTests.CastInternalCastFromMyType", () => {
    const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
    // This cast should be a no-op, since the interface matches the initial reference's exactly.
    const cast = castGrainReference(grain, ISimpleGrain);
    expect(cast).toBe(grain);
  });

  orleansTest("DefaultCluster.Tests.GrainReferenceCastTests.CastInternalCastUpFromChild", () => {
    const grain = cluster.getGrain(IGeneratorTestDerivedGrain1, randomIntegerKey());
    const cast = castGrainReference(grain, IGeneratorTestGrain);
    expect(grainReferenceIdentity(cast)?.grainId).toEqual(grainReferenceIdentity(grain)?.grainId);
    expect(grainReferenceIdentity(cast)?.interfaceId).toBe(IGeneratorTestGrain.id);
  });

  orleansTest("DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefUpCastFromChild", () => {
    const grain = cluster.getGrain(IGeneratorTestDerivedGrain1, randomIntegerKey());
    const cast = castGrainReference(grain, IGeneratorTestGrain);
    expect(grainReferenceIdentity(cast)?.grainId).toEqual(grainReferenceIdentity(grain)?.grainId);
    expect(grainReferenceIdentity(cast)?.interfaceId).toBe(IGeneratorTestGrain.id);
  });

  orleansTest("DefaultCluster.Tests.GrainReferenceCastTests.FailSideCastAfterResolve", async () => {
    const grain = cluster.getGrain(IGeneratorTestDerivedGrain1, randomIntegerKey());
    expect(await grain.stringIsNullOrEmpty()).toBe(true);

    const cast = castGrainReference(grain, IGeneratorTestDerivedGrain2);

    await expect(cast.stringConcat("a", "b", "c")).rejects.toThrow(GrainCallError);
  });

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.FailOperationAfterSideCast",
    async () => {
      const grain = cluster.getGrain(IGeneratorTestDerivedGrain1, randomIntegerKey());

      // Cast works optimistically when the grain reference is not already resolved.
      const cast = castGrainReference(grain, IGeneratorTestDerivedGrain2);

      // Operation fails when the call actually reaches the concrete grain.
      await expect(cast.stringConcat("a", "b", "c")).rejects.toThrow(GrainCallError);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.FailSideCastAfterContinueWith",
    async () => {
      const grain = cluster.getGrain(IGeneratorTestDerivedGrain1, randomIntegerKey());

      expect(await grain.stringIsNullOrEmpty()).toBe(true);

      // Casting is always allowed, so this should succeed.
      const cast = castGrainReference(grain, IGeneratorTestDerivedGrain2);

      // Call a method which the grain does not implement, resulting in a cast failure.
      await expect(cast.stringConcat("a", "b", "c")).rejects.toThrow(GrainCallError);

      // Call a method on the common interface, which the grain implements.
      // This should not throw.
      expect(await cast.stringIsNullOrEmpty()).toBe(true);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefUpCastFromGrandchild",
    () => {
      const grain = cluster.getGrain(IGeneratorTestDerivedDerivedGrain, randomIntegerKey());
      const grainIdentity = grainReferenceIdentity(grain);

      // Parent
      const parentCast = castGrainReference(grain, IGeneratorTestDerivedGrain2);
      expect(grainReferenceIdentity(parentCast)?.grainId).toEqual(grainIdentity?.grainId);
      expect(grainReferenceIdentity(parentCast)?.interfaceId).toBe(IGeneratorTestDerivedGrain2.id);

      // Grandparent
      const grandparentCast = castGrainReference(grain, IGeneratorTestGrain);
      expect(grainReferenceIdentity(grandparentCast)?.grainId).toEqual(grainIdentity?.grainId);
      expect(grainReferenceIdentity(grandparentCast)?.interfaceId).toBe(IGeneratorTestGrain.id);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefUpCastFromDerivedDerivedChild",
    () => {
      const grain = cluster.getGrain(IGeneratorTestDerivedDerivedGrain, randomIntegerKey());
      const cast = castGrainReference(grain, IGeneratorTestDerivedGrain2);
      expect(grainReferenceIdentity(cast)?.grainId).toEqual(grainReferenceIdentity(grain)?.grainId);
      expect(grainReferenceIdentity(cast)?.interfaceId).toBe(IGeneratorTestDerivedGrain2.id);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.CastAsyncGrainRefCastFromSelf",
    async () => {
      const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      const cast = castGrainReference(grain, ISimpleGrain);

      await expect(cast.getA()).resolves.toBe(0);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GrainReferenceCastTests.CastCallMethodInheritedFromBaseClass",
    async () => {
      const grain = cluster.getGrain(IGeneratorTestDerivedGrain1, randomIntegerKey());
      expect(await grain.stringIsNullOrEmpty()).toBe(true);

      await grain.stringSet("a");
      expect(await grain.stringIsNullOrEmpty()).toBe(false);

      await grain.stringSet("");
      expect(await grain.stringIsNullOrEmpty()).toBe(true);

      const cast = castGrainReference(grain, IGeneratorTestGrain);
      await cast.stringSet("b");
      expect(await grain.stringIsNullOrEmpty()).toBe(false);
    },
  );
});
