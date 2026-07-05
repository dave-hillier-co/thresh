// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/KeyExtensionTests.cs @ v10.1.0 (MIT).
// Every scenario but one turns on Orleans' compound key (`IGrainWithGuidCompoundKey`:
// a primary Guid plus a string "key extension"). This framework's key system
// (`GrainKeyFor<T>`, packages/core/src/key-kinds.ts) has only the three plain key
// kinds (string/integer/guid) with no compound-key variant, so
// `GetGrain<T>(guid, keyExtension, observer)` cannot be expressed at all yet.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { grainReferenceIdentity } from "@tsva/core/grain-reference";
import { IStringGrain, StringGrain } from "@tsva/parity/grains/impl/get-grain-grains";

describe("DefaultCluster.Tests.General.KeyExtensionTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: StringGrain, interfaces: [IStringGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

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

  // Upstream casts the grain reference to the concrete `GrainReference` and
  // calls `GetPrimaryKeyString()`; ported against `grainReferenceIdentity`.
  orleansTest(
    "DefaultCluster.Tests.General.KeyExtensionTests.GetPrimaryKeyStringOnGrainReference",
    () => {
      const key = "foo";

      const grain = cluster.getGrain(IStringGrain, key);
      const key2 = grainReferenceIdentity(grain)!.grainId.key;

      expect(key2).toBe(key);
    },
  );
});
