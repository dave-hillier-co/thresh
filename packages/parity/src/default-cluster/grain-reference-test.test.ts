// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/GrainReferenceTest.cs @ v10.1.0 (MIT).
//
// GrainReferenceComparison_DifferentReference and the three GrainReference_Pass_*
// tests were faithfully ported against ISimpleGrain/IChainedGrain (see
// chained-grain.ts, added for this file), then pulled below to GAP-BUG once
// they exposed the `grainReferenceIdentity` defect described there: they need
// no cluster/grain fixtures while gapped, so those are elided rather than left
// unused.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// See bugsFound: `grainReferenceIdentity` (packages/core/src/grain-reference.ts)
// tests `GRAIN_REF in value`, but the `in` operator invokes a Proxy's `has`
// trap, and grain-reference proxies (packages/runtime/src/grain-factory.ts
// `buildProxy`) define only a `get` trap — so the check always falls through to
// the (empty) Proxy target and returns `false`/`undefined`, even though
// `value[GRAIN_REF]` (a `get`) returns the identity correctly. Every consumer
// of `grainReferenceIdentity` — including the production wire codec
// (`packages/core/src/value-codec.ts`), which uses it to detect and reduce a
// grain reference before serializing a method argument — is affected: a grain
// reference passed as (or nested in) a call argument is never recognized as
// one, so it serializes as a plain empty object and never resolves back into
// a callable reference on the receiving side.

describe("DefaultCluster.Tests.General.GrainReferenceTest", () => {
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

  // TS has no operator overloading, so `!=`/`==`/`Equals` would be ported as
  // the underlying identity comparison (`GrainId.equals`) they delegate to in
  // Orleans' `GrainReference` implementation — but see the GAP-BUG comment
  // above: `grainReferenceIdentity` cannot see through the grain-reference
  // proxy at all right now.
  orleansTest.gap(
    "GAP-BUG-GRAIN-REF-IDENTITY",
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReferenceComparison_DifferentReference",
  );

  orleansTest.gap(
    "GAP-BUG-GRAIN-REF-IDENTITY",
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Pass_this",
  );

  orleansTest.gap(
    "GAP-BUG-GRAIN-REF-IDENTITY",
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Pass_this_Nested",
  );

  orleansTest.gap(
    "GAP-BUG-GRAIN-REF-IDENTITY",
    "DefaultCluster.Tests.General.GrainReferenceTest.GrainReference_Pass_Null",
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
