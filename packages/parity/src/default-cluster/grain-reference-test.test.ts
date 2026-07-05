// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/GrainReferenceTest.cs @ v10.1.0 (MIT).
//
// GrainReferenceComparison_DifferentReference and the three GrainReference_Pass_*
// tests are ported against ISimpleGrain/IChainedGrain (see chained-grain.ts,
// added for this file), now that `grainReferenceIdentity`
// (packages/core/src/grain-reference.ts) reads the `GRAIN_REF` property
// directly instead of testing `GRAIN_REF in value` — the `in` operator invokes
// a Proxy's `has` trap, and grain-reference proxies
// (packages/runtime/src/grain-factory.ts `buildProxy`) previously defined only
// a `get` trap, so the check always fell through to the (empty) Proxy target
// and returned `false`/`undefined` even though `value[GRAIN_REF]` (a `get`)
// returned the identity correctly. `buildProxy` now also implements a `has`
// trap for good measure, so `in` works too.
import { afterAll, beforeAll, describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { TestCluster } from "@tsva/testing/test-cluster";
import { grainReferenceIdentity } from "@tsva/core/grain-reference";
import { ISimpleGrain, SimpleGrain } from "@tsva/parity/grains/impl/simple-grain";
import { ChainedGrain, IChainedGrain } from "@tsva/parity/grains/impl/chained-grain";
import { randomIntegerKey } from "@tsva/parity/support/keys";

describe("DefaultCluster.Tests.General.GrainReferenceTest", () => {
  let cluster: TestCluster;

  beforeAll(async () => {
    cluster = await TestCluster.start({
      initialSilos: 2,
      grains: [
        { ctor: SimpleGrain, interfaces: [ISimpleGrain] },
        { ctor: ChainedGrain, interfaces: [IChainedGrain] },
      ],
    });
  });

  afterAll(async () => {
    await cluster.dispose();
  });

  // Upstream hard-codes 3643965955u, the output of Orleans' own uniform-hash
  // implementation (a specific hash algorithm over its own grain-id wire
  // encoding) applied to a `GrainReference` cast. This framework's uniform hash
  // (`GrainId.getUniformHashCode`, packages/core/src/grain-id.ts) is a different
  // algorithm over a different id encoding, so the literal cannot be reproduced
  // without reimplementing Orleans' internal hash function — out of scope for a
  // parity port.
  orleansTest.excluded(
    "asserts a literal output of Orleans' own internal uniform-hash algorithm over its grain-id wire encoding; this framework's hash is a different algorithm/encoding entirely, so the magic number cannot be reproduced without reimplementing Orleans internals",
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReferenceComparison_ShouldProduceUniformHashCode",
  );

  // TS has no operator overloading, so `!=`/`==`/`Equals` are ported as the
  // underlying identity comparison (`GrainId.equals`) they delegate to in
  // Orleans' `GrainReference` implementation.
  orleansTest(
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReferenceComparison_DifferentReference",
    () => {
      const ref1 = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      const ref2 = cluster.getGrain(ISimpleGrain, randomIntegerKey());
      const id1 = grainReferenceIdentity(ref1)!.grainId;
      const id2 = grainReferenceIdentity(ref2)!.grainId;
      expect(id1.equals(id2)).toBe(false);
      expect(id2.equals(id1)).toBe(false);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Pass_this",
    async () => {
      const g1 = cluster.getGrain(IChainedGrain, randomIntegerKey());
      const g2 = cluster.getGrain(IChainedGrain, randomIntegerKey());

      await g1.passThis(g2);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Pass_this_Nested",
    async () => {
      const g1 = cluster.getGrain(IChainedGrain, randomIntegerKey());
      const g2 = cluster.getGrain(IChainedGrain, randomIntegerKey());

      await g1.passThisNested({ next: g2 });
    },
  );

  orleansTest(
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Pass_Null",
    async () => {
      const g1 = cluster.getGrain(IChainedGrain, randomIntegerKey());
      const g2 = cluster.getGrain(IChainedGrain, randomIntegerKey());

      // g1 will pass a null reference to g2
      await g1.passNullNested({ next: g2 });
      expect(await g2.getNext()).toBeNull();
      await g1.passNull(g2);
      expect(await g2.getNext()).toBeNull();
    },
  );

  const jsonSerializationReason =
    "round-trips a GrainReference through Newtonsoft.Json using Orleans' custom JsonConverter (OrleansJsonSerializerSettings); this framework has no JSON serializer layer for grain references — the wire codec (@tsva/messaging serializer) is exercised directly by every ported test that passes a grain reference, e.g. GrainReference_Pass_this above";

  orleansTest.excluded(
    jsonSerializationReason,
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Json_Serialization",
  );
  orleansTest.excluded(
    jsonSerializationReason,
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Json_Serialization_Nested",
  );
  orleansTest.excluded(
    jsonSerializationReason,
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Json_Serialization_Unresolved",
  );

  orleansTest.excluded(
    'skipped upstream: "GrainReference interning is not currently implemented."',
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Interning_Sys_StoreGrain",
  );
});
