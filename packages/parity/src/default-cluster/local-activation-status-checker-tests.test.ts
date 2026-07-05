// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/LocalActivationStatusCheckerTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { grainReferenceIdentity } from "@tsva/core/grain-reference";
import type { GrainId } from "@tsva/core/grain-id";
import { ISimpleGrain, SimpleGrain } from "@tsva/parity/grains/impl/simple-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

// This in-process test harness has no separate "client" process distinct from
// a silo: `TestCluster.getGrain` proxies calls straight through the primary
// silo's own `SiloHost`, which is exactly the object `isActive` is checked
// against for the "locally activated" assertions above. There is therefore no
// object anywhere in this framework that models Orleans' client-side
// `ILocalActivationStatusChecker` (which always answers `false` because a
// client never hosts activations) as something distinct from the silo-side
// checker.

describe("DefaultCluster.Tests.LocalActivationStatusCheckerTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: SimpleGrain, interfaces: [ISimpleGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  const grainIdOf = (ref: unknown): GrainId => grainReferenceIdentity(ref)!.grainId;

  orleansTest(
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ShouldReturnTrueForLocallyActivatedGrain",
    async () => {
      const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      await grain.setA(42);

      const grainId = grainIdOf(grain);
      const activatedOn = cluster.silos.filter((silo) => silo.host.isActive(grainId));
      expect(activatedOn).toHaveLength(1);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ShouldReturnFalseForNonActivatedGrain",
    () => {
      const grain = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      const grainId = grainIdOf(grain);
      expect(cluster.silos.some((silo) => silo.host.isActive(grainId))).toBe(false);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ShouldReturnFalseForDifferentGrainIdentity",
    async () => {
      const grain1 = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      await grain1.setA(42);

      const grain2 = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      const grainId2 = grainIdOf(grain2);
      expect(cluster.silos.some((silo) => silo.host.isActive(grainId2))).toBe(false);
    },
  );

  orleansTest.gap(
    "GAP-CLIENT-SILO-SEPARATION",
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ClientShouldAlwaysReturnFalseForIsLocallyActivated",
  );

  orleansTest.gap(
    "GAP-CLIENT-SILO-SEPARATION",
    "DefaultCluster.Tests.LocalActivationStatusCheckerTests.ClientShouldReturnFalseForNonActivatedGrain",
  );
});
