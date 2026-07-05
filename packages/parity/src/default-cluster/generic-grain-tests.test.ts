// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/GenericGrainTests.cs @ v10.1.0 (MIT).
//
// This file is almost entirely generic grain interfaces (`IGenericGrain<T,U>`,
// `ISimpleGenericGrain<T>`, ...): open generic grain interfaces are
// unrepresentable in this framework (GAP-GENERIC-GRAINS — the grain-interface
// registry keys interfaces by a fixed string name/id, with no notion of a type
// parameter to instantiate), so nearly every case here is gapped under that
// tag. Only the handful of tests that exercise a genuinely non-generic grain
// (GrainWithNoProperties, GrainWithListFields, ValueTypeTestGrain) or a
// different, already-tagged gap are ported or otherwise accounted for below.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  GrainWithListFields,
  GrainWithNoProperties,
  IGrainWithListFields,
  IGrainWithNoProperties,
} from "@tsva/parity/grains/impl/generic-grain-tests-grains";
import {
  IValueTypeTestGrain,
  ValueTypeTestGrain,
} from "@tsva/parity/grains/impl/value-type-test-grain";
import { randomIntegerKey, randomGuidKey } from "@tsva/parity/support/keys";

describe("DefaultCluster.Tests.General.GenericGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: GrainWithNoProperties, interfaces: [IGrainWithNoProperties] },
        { ctor: GrainWithListFields, interfaces: [IGrainWithListFields] },
        { ctor: ValueTypeTestGrain, interfaces: [IValueTypeTestGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_ConcreteGrainWithGenericInterfaceGetGrain",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_ConcreteGrainWithGenericInterfaceMultiplicity",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_SimpleGenericGrainGetGrain",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_SimpleGenericGrainGetGrain_ArrayTypeParameter",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_GenericGrainInheritingArray",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_GenericInterfaceWithGenericParametersGetGrain",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_SimpleGenericGrainMultiplicity",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_PreferConcreteGrainImplementationOfGenericInterface",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_PreferConcreteGrainImplementationOfGenericInterfaceMultiplicity",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_ConcreteGrainWithMultipleGenericInterfacesGetGrain",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_ConcreteGrainWithMultipleGenericInterfacesMultiplicity1",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_ConcreteGrainWithMultipleGenericInterfacesMultiplicity2",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainTests_UseGenericFactoryInsideGrain",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_SimpleGrain_GetGrain",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_SimpleGrainControlFlow",
  );

  orleansTest.excluded(
    "blocks the calling thread on `.Wait()`/`.Result` to prove Orleans' client scheduler does not deadlock; this framework has no synchronous blocking call surface (every grain call is a Promise) so there is no equivalent deadlock hazard to demonstrate, and it is also GAP-GENERIC-GRAINS (ISimpleGenericGrain1<int>)",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_SimpleGrainControlFlow_Blocking",
  );

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_SimpleGrainDataFlow",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_SimpleGrain2_GetGrain",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_SimpleGrainGenericParameterWithMultipleArguments_GetGrain",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_SimpleGrainControlFlow2_GetAB",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_SimpleGrainControlFlow3",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_BasicGrainControlFlow",
  );

  orleansTest("DefaultCluster.Tests.General.GenericGrainTests.GrainWithListFields", async () => {
    const a = "a-item";
    const b = "b-item";

    const g1 = cluster.getGrain(IGrainWithListFields, randomIntegerKey());

    const p1 = g1.addItem(a);
    const p2 = g1.addItem(b);
    await Promise.all([p1, p2]);

    const r1 = await g1.getItems();

    expect((a === r1[0] && b === r1[1]) || (b === r1[0] && a === r1[1])).toBe(true);
  });

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_GrainWithListFields",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_GrainWithNoProperties_ControlFlow",
  );

  orleansTest(
    "DefaultCluster.Tests.General.GenericGrainTests.GrainWithNoProperties_ControlFlow",
    async () => {
      const a = 12;
      const b = 34;
      const expected = `${a}x${b}`;

      const g1 = cluster.getGrain(IGrainWithNoProperties, randomIntegerKey());

      const r1 = await g1.getAxB(a, b);
      expect(r1).toBe(expected);
    },
  );

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_ReaderWriterGrain1",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_ReaderWriterGrain2",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_ReaderWriterGrain3",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_Non_Primitive_Type_Argument",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_Echo_Chain_1",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_Echo_Chain_2",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_Echo_Chain_3",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_Echo_Chain_4",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_Echo_Chain_5",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_Echo_Chain_6",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_1Argument_GenericCallOnly",
  );

  orleansTest.excluded(
    "relies on Orleans' reflection-based ambiguous-implementation resolution (GetGrain(key, fullName) picking a generic grain class by name) and IGeneric1Argument<T>'s open generic interface; this framework binds an interface to one implementation statically at registration with no runtime type scan, and has no open-generic-interface concept (GAP-GENERIC-GRAINS) either",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_1Argument_NonGenericCallFirst",
  );

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_1Argument_GenericCallFirst",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.DifferentTypeArgsProduceIndependentActivations",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_PingSelf",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_PingOther",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_PingSelfThroughOther",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_ScheduleDelayedPingAndDeactivate",
  );

  orleansTest.gap(
    "GAP-COMPOUND-KEY",
    "DefaultCluster.Tests.General.GenericGrainTests.SerializationTests_Generic_CircularReferenceTest",
  );

  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_GrainWithTypeConstraints",
  );

  orleansTest(
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_GrainWithValueTypeState",
    async () => {
      const grain = cluster.getGrain(IValueTypeTestGrain, randomGuidKey());

      const initial = await grain.getStateData();
      expect(initial).toEqual({ value: 0 });

      const expectedValue = { value: 42 };
      await grain.setStateData(expectedValue);

      expect(await grain.getStateData()).toEqual(expectedValue);
    },
  );

  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_CastToGenericInterfaceAfterActivation",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_CastToDifferentlyConcretizedGenericInterfaceBeforeActivation",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_CastToDifferentlyConcretizedInterfaceBeforeActivation",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.General.GenericGrainTests.Generic_CastGenericInterfaceToNonGenericInterfaceBeforeActivation",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.GenericGrainTests.GenericGrainStateParameterMismatchTest",
  );
});

describe("DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests", () => {
  const skippedUpstreamReason = 'skipped upstream: [Fact(Skip = "Currently unsupported")]';

  orleansTest.excluded(
    skippedUpstreamReason,
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_PartiallySpecifyingGenericGrainFulfilsInterface",
  );
  orleansTest.excluded(
    skippedUpstreamReason,
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_GenericGrainCanReuseOwnGenArgRepeatedly",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_PartiallySpecifyingGenericInterfaceIsCastable",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_PartiallySpecifyingGenericInterfaceIsCastable_Activating",
  );
  orleansTest.excluded(
    skippedUpstreamReason,
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_RepeatedRearrangedGenArgsResolved",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_RepeatedGenArgsWorkAmongstInterfacesInTypeResolution",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_RepeatedGenArgsWorkAmongstInterfacesInCasting",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_RepeatedGenArgsWorkAmongstInterfacesInCasting_Activating",
  );
  orleansTest.excluded(
    skippedUpstreamReason,
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_RearrangedGenArgsOfCorrectArityAreResolved",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_RearrangedGenArgsOfCorrectNumberAreCastable",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_RearrangedGenArgsOfCorrectNumberAreCastable_Activating",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.CastingBetweenFullySpecifiedGenericInterfaces",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_CanCastToFullySpecifiedInterfaceUnrelatedToConcreteGenArgs",
  );
  orleansTest.gap(
    "GAP-GENERIC-GRAINS",
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_CanCastToFullySpecifiedInterfaceUnrelatedToConcreteGenArgs_Activating",
  );
  orleansTest.excluded(
    skippedUpstreamReason,
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_GenArgsCanBeFurtherSpecialized",
  );
  orleansTest.excluded(
    skippedUpstreamReason,
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_GenArgsCanBeFurtherSpecializedIntoArrays",
  );
  orleansTest.excluded(
    skippedUpstreamReason,
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_CanCastBetweenInterfacesWithFurtherSpecializedGenArgs",
  );
  orleansTest.excluded(
    skippedUpstreamReason,
    "DefaultCluster.Tests.General.Generic.EdgeCases.GenericEdgeCaseTests.Generic_CanCastBetweenInterfacesWithFurtherSpecializedGenArgs_Activating",
  );
});
