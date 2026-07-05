// Ported from dotnet/orleans test/Orleans.EventSourcing.Tests/EventSourcingTests/PersonGrainTests.cs @ v10.1.0 (MIT).
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { IPersonGrain, PersonGrain } from "@tsva/parity/grains/impl/person-grain";
import { Guid } from "@tsva/core/guid";
import { randomGuidKey } from "@tsva/parity/support/keys";

// Already reported (default-cluster/grain-reference-test.test.ts): `grainReferenceIdentity`
// (packages/core/src/grain-reference.ts) tests `GRAIN_REF in value`, but the `in`
// operator invokes a Proxy's `has` trap and the grain-reference proxy
// (packages/runtime/src/grain-factory.ts `buildProxy`) defines only a `get`
// trap, so the check always falls through to the (empty) proxy target and
// returns `false` — a grain reference passed as a call argument (`leia.marry(han)`)
// is never recognized and serializes as a plain empty object.

describe("Tester.EventSourcingTests.PersonGrainTests", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [{ ctor: PersonGrain, interfaces: [IPersonGrain] }],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  orleansTest(
    "Tester.EventSourcingTests.PersonGrainTests.JournaledGrainTests_Activate",
    async () => {
      const grainWithState = cluster.getGrain(
        IPersonGrain,
        Guid.parse("00000000-0000-0000-0000-000000000000"),
      );

      expect(await grainWithState.getTentativePersonalAttributes()).not.toBeNull();
    },
  );

  orleansTest(
    "Tester.EventSourcingTests.PersonGrainTests.JournaledGrainTests_Persist",
    async () => {
      const grainWithState = cluster.getGrain(IPersonGrain, randomGuidKey());

      await grainWithState.registerBirth({
        firstName: "Luke",
        lastName: "Skywalker",
        gender: "Male",
      });

      const attributes = await grainWithState.getTentativePersonalAttributes();

      expect(attributes).not.toBeNull();
      expect(attributes.firstName).toBe("Luke");
    },
  );

  orleansTest.gap(
    "GAP-BUG-GRAIN-REF-IDENTITY",
    "Tester.EventSourcingTests.PersonGrainTests.JournaledGrainTests_AppendMoreEvents",
    async () => {
      const leia = cluster.getGrain(IPersonGrain, randomGuidKey());
      await leia.registerBirth({ firstName: "Leia", lastName: "Organa", gender: "Female" });

      const han = cluster.getGrain(IPersonGrain, randomGuidKey());
      await han.registerBirth({ firstName: "Han", lastName: "Solo", gender: "Male" });

      await leia.marry(han);

      const attributes = await leia.getTentativePersonalAttributes();
      expect(attributes).not.toBeNull();
      expect(attributes.firstName).toBe("Leia");
      expect(attributes.lastName).toBe("Solo");
    },
  );

  // Requires `Version`/`TentativeVersion`/`ConfirmEvents` from the missing
  // journaled-grain machinery (GAP-EVENT-SOURCING).
  orleansTest.gap(
    "GAP-EVENT-SOURCING",
    "Tester.EventSourcingTests.PersonGrainTests.JournaledGrainTests_TentativeConfirmedState",
  );
});
