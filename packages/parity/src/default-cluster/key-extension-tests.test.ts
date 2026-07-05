// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/KeyExtensionTests.cs @ v10.1.0 (MIT).
// Every scenario but one turns on Orleans' compound key (`IGrainWithGuidCompoundKey`:
// a primary Guid plus a string "key extension"). This framework's key system
// (`GrainKeyFor<T>`, packages/core/src/key-kinds.ts) has only the three plain key
// kinds (string/integer/guid) with no compound-key variant, so
// `GetGrain<T>(guid, keyExtension, observer)` cannot be expressed at all yet.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// GetPrimaryKeyStringOnGrainReference (upstream casts the grain reference to
// the concrete `GrainReference` and calls `GetPrimaryKeyString()`) was
// faithfully ported against IStringGrain via `grainReferenceIdentity`, then
// pulled below to GAP-BUG-GRAIN-REF-IDENTITY (see grain-reference-test.test.ts
// for the full defect writeup: `grainReferenceIdentity` uses the `in` operator
// against a Proxy that defines only a `get` trap, so it always returns
// `undefined`) — the cluster/grain fixture it needed is elided rather than
// left unused.

describe("DefaultCluster.Tests.General.KeyExtensionTests", () => {
  orleansTest.gap(
    "GAP-COMPOUND-KEY",
    "DefaultCluster.Tests.General.KeyExtensionTests.PrimaryKeyExtensionsShouldDifferentiateGrainsUsingTheSameBasePrimaryKey",
  );
  orleansTest.gap(
    "GAP-COMPOUND-KEY",
    "DefaultCluster.Tests.General.KeyExtensionTests.PrimaryKeyExtensionsShouldDifferentiateGrainsUsingDifferentBaseKeys",
  );
  orleansTest.gap(
    "GAP-COMPOUND-KEY",
    "DefaultCluster.Tests.General.KeyExtensionTests.EmptyKeyExtensionsAreDisallowed",
  );
  orleansTest.gap(
    "GAP-COMPOUND-KEY",
    "DefaultCluster.Tests.General.KeyExtensionTests.WhiteSpaceKeyExtensionsAreDisallowed",
  );
  orleansTest.gap(
    "GAP-COMPOUND-KEY",
    "DefaultCluster.Tests.General.KeyExtensionTests.NullKeyExtensionsAreDisallowed",
  );
  orleansTest.gap(
    "GAP-COMPOUND-KEY",
    "DefaultCluster.Tests.General.KeyExtensionTests.PrimaryKeyExtensionsShouldPermitStringsLongerThan127BytesLong",
  );

  orleansTest.gap(
    "GAP-BUG-GRAIN-REF-IDENTITY",
    "DefaultCluster.Tests.General.KeyExtensionTests.GetPrimaryKeyStringOnGrainReference",
  );
});
