// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/KeyExtensionTests.cs @ v10.1.0 (MIT).
// Every scenario but one turns on Orleans' compound key (`IGrainWithGuidCompoundKey`:
// a primary Guid plus a string "key extension"), represented here by
// `CompoundKey<Guid>` (packages/core/src/grain-key.ts) via the
// `GrainKey<CompoundKey<Guid>>` marker interface (packages/core/src/key-kinds.ts).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import { TestCluster } from "@thresh/testing/test-cluster";
import { CompoundKey } from "@thresh/core/grain-key";
import { grainReferenceIdentity } from "@thresh/core/grain-reference";
import { Guid } from "@thresh/core/guid";
import { IStringGrain, StringGrain } from "@thresh/parity/grains/impl/get-grain-grains";
import {
  IKeyExtensionTestGrain,
  KeyExtensionTestGrain,
} from "@thresh/parity/grains/impl/key-extension-test-grain";

describe("DefaultCluster.Tests.General.KeyExtensionTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: StringGrain, interfaces: [IStringGrain] },
        { ctor: KeyExtensionTestGrain, interfaces: [IKeyExtensionTestGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "DefaultCluster.Tests.General.KeyExtensionTests.PrimaryKeyExtensionsShouldDifferentiateGrainsUsingTheSameBasePrimaryKey",
    async () => {
      const baseKey = Guid.newGuid();

      const kx1 = "1";
      const kx2 = "2";

      const grain1 = cluster.getGrain(IKeyExtensionTestGrain, new CompoundKey(baseKey, kx1));
      const grainId1 = grainReferenceIdentity(await grain1.getGrainReference())!.grainId;
      const activationId1 = await grain1.getActivationId();

      const grain2 = cluster.getGrain(IKeyExtensionTestGrain, new CompoundKey(baseKey, kx2));
      const grainId2 = grainReferenceIdentity(await grain2.getGrainReference())!.grainId;
      const activationId2 = await grain2.getActivationId();

      // Mismatched key extensions should differentiate an identical base primary key.
      expect(grainId1.equals(grainId2)).toBe(false);
      expect(activationId1).not.toBe(activationId2);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.KeyExtensionTests.PrimaryKeyExtensionsShouldDifferentiateGrainsUsingDifferentBaseKeys",
    async () => {
      const baseKey1 = Guid.newGuid();
      const baseKey2 = Guid.newGuid();

      const kx = "1";

      const grain1 = cluster.getGrain(IKeyExtensionTestGrain, new CompoundKey(baseKey1, kx));
      const grainId1 = grainReferenceIdentity(await grain1.getGrainReference())!.grainId;
      const activationId1 = await grain1.getActivationId();

      const grain2 = cluster.getGrain(IKeyExtensionTestGrain, new CompoundKey(baseKey2, kx));
      const grainId2 = grainReferenceIdentity(await grain2.getGrainReference())!.grainId;
      const activationId2 = await grain2.getActivationId();

      // Mismatched base keys should differentiate between identical extended keys.
      expect(grainId1.equals(grainId2)).toBe(false);
      expect(activationId1).not.toBe(activationId2);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.KeyExtensionTests.EmptyKeyExtensionsAreDisallowed",
    () => {
      const baseKey = Guid.newGuid();

      expect(() => new CompoundKey(baseKey, "")).toThrow();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.KeyExtensionTests.WhiteSpaceKeyExtensionsAreDisallowed",
    () => {
      const baseKey = Guid.newGuid();

      expect(() => new CompoundKey(baseKey, " \t\n\r")).toThrow();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.KeyExtensionTests.NullKeyExtensionsAreDisallowed",
    () => {
      const baseKey = Guid.newGuid();

      // Upstream expects ArgumentNullException specifically for a null
      // extension; the type system already forbids passing `null` for the
      // (non-nullable) `extension: string` parameter, so callers can't reach
      // this path without a cast — exercised here the same way other ported
      // tests probe runtime guards against statically-disallowed inputs.
      expect(() => new CompoundKey(baseKey, null as unknown as string)).toThrow();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.KeyExtensionTests.PrimaryKeyExtensionsShouldPermitStringsLongerThan127BytesLong",
    async () => {
      const baseKey = Guid.newGuid();

      const kx1 = "\\".repeat(300);

      const localGrainRef = cluster.getGrain(IKeyExtensionTestGrain, new CompoundKey(baseKey, kx1));
      const remoteGrainRef = await localGrainRef.getGrainReference();

      // Mismatched grain ID.
      expect(
        grainReferenceIdentity(remoteGrainRef)!.grainId.equals(
          grainReferenceIdentity(localGrainRef)!.grainId,
        ),
      ).toBe(true);
    },
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
