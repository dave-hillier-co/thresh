// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/CodeGenTests/CodeGeneratorTests_KnownAssemblyAttribute.cs @ v10.1.0 (MIT).
//
// Every test here checks whether the Orleans serializer's compile-time codegen
// registry has generated a serializer for a given .NET `Type` — either inside
// a grain (`ISerializerPresenceTest.SerializerExistsForType`) or on the client
// (`HostedCluster.GetSerializer().CanSerialize(Type)`) — for types living in the
// grain assembly, a "known assembly" (F#'s `FSharpOption<int>`), and a grain
// assembly carrying `[KnownAssembly]` for F# interop. `[KnownAssembly]`,
// per-.NET-assembly serializer registries, and `Type`-keyed "can this be
// serialized" introspection are all specific to Orleans' Roslyn source
// generator and .NET's `System.Type` reflection; this TypeScript framework has
// no assemblies, no source-generator registry, and no F# interop to test.
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const reason =
  "Tests whether Orleans' Roslyn-generated serializer registry (optionally extended via " +
  "[KnownAssembly] for F# interop) has a serializer for a given .NET Type, on the silo and " +
  "on the client; this framework has no per-assembly codegen serializer registry, no " +
  "System.Type reflection, and no F# interop.";

describe("DefaultCluster.Tests.General.KnownAssemblyAttributeTests", () => {
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.KnownAssemblyAttributeTests.Silo_Serializer_Exists_for_Type_In_Grain_Assembly",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.KnownAssemblyAttributeTests.Client_Serializer_Exists_for_Type_In_Grain_Assembly",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.KnownAssemblyAttributeTests.Silo_Serializer_Exists_for_Type_In_Known_Assembly",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.KnownAssemblyAttributeTests.Client_Serializer_Exists_for_Type_In_Known_Assembly",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.KnownAssemblyAttributeTests.Silo_Serializer_Exists_for_Type_In_Grain_Assembly_containing_KnownAssemblyAttribute",
  );
  orleansTest.excluded(
    reason,
    "DefaultCluster.Tests.General.KnownAssemblyAttributeTests.Client_Serializer_Exists_for_Type_In_Grain_Assembly_containing_KnownAssemblyAttribute",
  );

  // Every upstream Fact in this file is excluded (above); this placeholder
  // keeps the suite non-empty for the test runner and is not itself part of
  // the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
