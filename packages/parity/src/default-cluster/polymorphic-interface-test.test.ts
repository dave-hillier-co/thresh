// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/PolymorphicInterfaceTest.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  IA,
  IB,
  IC,
  ID,
  IE,
  IF,
  PolymorphicTestGrain,
} from "@tsva/parity/grains/impl/polymorphic-test-grain";
import {
  DerivedServiceType,
  IDerivedServiceType,
  IServiceType,
  ServiceType,
} from "@tsva/parity/grains/impl/service-type-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

describe("DefaultCluster.Tests.General.PolymorphicInterfaceTest", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: PolymorphicTestGrain, interfaces: [IA, IB, IC, ID, IE, IF] },
        { ctor: ServiceType, interfaces: [IServiceType] },
        { ctor: DerivedServiceType, interfaces: [IDerivedServiceType] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.General.PolymorphicInterfaceTest.Polymorphic_SimpleTest",
    async () => {
      const iaRef = cluster.getGrain(IA, randomIntegerKey());
      expect(await iaRef.a1Method()).toBe("A1");
      expect(await iaRef.a2Method()).toBe("A2");
      expect(await iaRef.a3Method()).toBe("A3");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.PolymorphicInterfaceTest.Polymorphic_UpCastTest",
    async () => {
      // Upstream `IA IARef = ICRef;` is a compile-time interface upcast (IC
      // extends IA/IB); this framework's grain interfaces mirror that
      // extension structurally, so the reference itself already exposes the
      // base-interface methods with no separate factory call or cast.
      const icRef = cluster.getGrain(IC, randomIntegerKey());
      expect(await icRef.a1Method()).toBe("A1");
      expect(await icRef.a2Method()).toBe("A2");
      expect(await icRef.a3Method()).toBe("A3");
      expect(await icRef.b1Method()).toBe("B1");
      expect(await icRef.b2Method()).toBe("B2");
      expect(await icRef.b3Method()).toBe("B3");

      const ifRef = cluster.getGrain(IF, randomIntegerKey());
      expect(await ifRef.f1Method()).toBe("F1");
      expect(await ifRef.f2Method()).toBe("F2");
      expect(await ifRef.f3Method()).toBe("F3");
      expect(await ifRef.e1Method()).toBe("E1");
      expect(await ifRef.e2Method()).toBe("E2");
      expect(await ifRef.e3Method()).toBe("E3");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.PolymorphicInterfaceTest.Polymorphic_FactoryMethods",
    async () => {
      // FRef factory method returns a polymorphic reference to ICRef.
      const icViaIf = cluster.getGrain(IF, randomIntegerKey());
      expect(await icViaIf.b2Method()).toBe("B2");

      // DRef factory method returns a polymorphic reference to IARef.
      const iaViaId = cluster.getGrain(ID, randomIntegerKey());
      expect(await iaViaId.a1Method()).toBe("A1");
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.PolymorphicInterfaceTest.Polymorphic_ServiceType",
    async () => {
      const serviceRef = cluster.getGrain(IServiceType, randomIntegerKey());
      expect(await serviceRef.a1Method()).toBe("A1");
      expect(await serviceRef.a2Method()).toBe("A2");
      expect(await serviceRef.a3Method()).toBe("A3");
      expect(await serviceRef.b1Method()).toBe("B1");
      expect(await serviceRef.b2Method()).toBe("B2");
      expect(await serviceRef.b3Method()).toBe("B3");
    },
  );

  orleansTest.excluded(
    "relies on C#'s explicit interface implementation, giving one grain method " +
      "name (CommonMethod) a different body per caller-side interface type " +
      "(IA/IB/IC); this framework dispatches by method name on the grain " +
      "instance with no notion of which interface the caller views the " +
      "reference through, so one grain cannot have three bodies for one name",
    "DefaultCluster.Tests.General.PolymorphicInterfaceTest.Polymorphic_InheritedMethodAmbiguity",
  );

  orleansTest(
    "DefaultCluster.Tests.General.PolymorphicInterfaceTest.Polymorphic__DerivedServiceType",
    async () => {
      const derivedRef = cluster.getGrain(IDerivedServiceType, randomIntegerKey());

      expect(await derivedRef.a1Method()).toBe("A1");
      expect(await derivedRef.a2Method()).toBe("A2");
      expect(await derivedRef.a3Method()).toBe("A3");

      expect(await derivedRef.b1Method()).toBe("B1");
      expect(await derivedRef.b2Method()).toBe("B2");
      expect(await derivedRef.b3Method()).toBe("B3");

      expect(await derivedRef.f1Method()).toBe("F1");
      expect(await derivedRef.f2Method()).toBe("F2");
      expect(await derivedRef.f3Method()).toBe("F3");

      expect(await derivedRef.e1Method()).toBe("E1");
      expect(await derivedRef.e2Method()).toBe("E2");
      expect(await derivedRef.e3Method()).toBe("E3");

      expect(await derivedRef.h1Method()).toBe("H1");
      expect(await derivedRef.h2Method()).toBe("H2");
      expect(await derivedRef.h3Method()).toBe("H3");

      expect(await derivedRef.serviceTypeMethod1()).toBe("ServiceTypeMethod1");
      expect(await derivedRef.serviceTypeMethod2()).toBe("ServiceTypeMethod2");
      expect(await derivedRef.serviceTypeMethod3()).toBe("ServiceTypeMethod3");

      expect(await derivedRef.derivedServiceTypeMethod1()).toBe("DerivedServiceTypeMethod1");
    },
  );
});
