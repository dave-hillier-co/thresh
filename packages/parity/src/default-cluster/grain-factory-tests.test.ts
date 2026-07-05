// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/GrainFactoryTests.cs @ v10.1.0 (MIT).
//
// Most of this file exercises Orleans' reflection-based ambiguous-implementation
// resolution: GetGrain<T>(key, grainClassNamePrefix) / GetGrain<T>(key, grainFullName)
// scan the loaded assemblies for grain classes implementing T and disambiguate by
// class-name prefix or full name, throwing ArgumentException on ambiguity or a
// missing match. This framework's grain factory (`cluster.getGrain(def, key)`)
// binds an interface to exactly one implementation statically, at registration
// time, via the TestCluster's `grains` option — there is no runtime type scan, no
// prefix/full-name string API, and therefore no way for a lookup to be
// "ambiguous". The non-generic `GetGrain(Type, ...)` overload and
// `GrainIdKeyExtensions.CreateIntegerKey` span-based key construction are the
// same .NET-reflection surface and are excluded for the same reason. Only the
// two key-kind probes (string- and guid-keyed grains) test something this
// framework's static API can express.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import {
  GuidGrain,
  IGuidGrain,
  IStringGrain,
  StringGrain,
} from "@tsva/parity/grains/impl/get-grain-grains";
import { Guid } from "@tsva/core/guid";

describe("DefaultCluster.Tests.GrainFactoryTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: StringGrain, interfaces: [IStringGrain] },
        { ctor: GuidGrain, interfaces: [IGuidGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  const ambiguousResolutionReason =
    "relies on Orleans' reflection-based ambiguous-implementation resolution (GetGrain(key, classPrefix|fullName)); this framework binds an interface to one implementation statically at registration, so there is no runtime type scan to disambiguate";

  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_Ambiguous",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_Ambiguous_WithDefault",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_WithFullName",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_WithPrefix",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_AmbiguousPrefix",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_WrongPrefix",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_Derived_NoPrefix",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_Derived_WithFullName",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_Derived_WithFullName_FromBase",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_Derived_WithPrefix",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_Derived_WithWrongPrefix",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_OneImplementation_NoPrefix",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_OneImplementation_Prefix",
  );
  orleansTest.excluded(
    ambiguousResolutionReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrain_MultipleUnrelatedInterfaces",
  );

  orleansTest("DefaultCluster.Tests.GrainFactoryTests.GetStringGrain", async () => {
    const g = cluster.getGrain(IStringGrain, Guid.newGuid().toString());
    expect(await g.foo()).toBe(true);
  });

  orleansTest("DefaultCluster.Tests.GrainFactoryTests.GetGuidGrain", async () => {
    const g = cluster.getGrain(IGuidGrain, Guid.newGuid());
    expect(await g.foo()).toBe(true);
  });

  const grainIdSpanReason =
    "relies on GrainIdKeyExtensions.CreateIntegerKey span-based key construction and the non-generic GetGrain(Type, key[, prefix]) overload, both reflection-based .NET APIs this framework's typed getGrain(def, key) has no analogue for";

  orleansTest.excluded(
    grainIdSpanReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrainIdSpanGeneric",
  );
  orleansTest.excluded(grainIdSpanReason, "DefaultCluster.Tests.GrainFactoryTests.GetGrainIdSpan");
  orleansTest.excluded(
    grainIdSpanReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrainIdSpanGenericPrefix",
  );
  orleansTest.excluded(
    grainIdSpanReason,
    "DefaultCluster.Tests.GrainFactoryTests.GetGrainIdSpanPrefix",
  );
});
