// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/MultifacetGrainTest.cs @ v10.1.0 (MIT).
//
// RWReferences / RWReferencesInvalidCastException turn on
// `GrainReference.AsReference<T>()` re-typing an already-obtained reference to
// a sibling interface it wasn't fetched through — the same gap the sibling
// `grain-reference-cast-tests.test.ts` file already carries under
// GAP-GRAIN-REF-CAST, so they are gapped under the same tag rather than a new
// one.
//
// MultifacetFactory / Multifacet_InterfacesAsArguments faithfully port to a
// grain call that passes a grain reference as an argument and gets one back
// as a result (`packages/parity/src/grains/impl/multifacet-grain.ts`) — and
// both hit the pre-existing `grainReferenceIdentity` defect
// (packages/core/src/grain-reference.ts, also described in
// grain-reference-test.test.ts): it tests `GRAIN_REF in value` against a grain
// reference `Proxy`, but the proxy only defines a `get` trap, so the default
// `has` trap forwards to the (empty) proxy target and the check always misses.
// A grain reference that crosses a call boundary as an argument or return
// value is therefore never recognised as one, gets reduced to plain data by
// the wire codec, and arrives with none of its methods — so calling
// `writer.setValue(...)` on the result throws `TypeError: writer.setValue is
// not a function`. Gapped rather than excluded per the framework-bug rule;
// there is no live cluster fixture here since every case in this file is a
// gap.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.General.MultifacetGrainTest", () => {
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.General.MultifacetGrainTest.RWReferences",
  );

  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.General.MultifacetGrainTest.RWReferencesInvalidCastException",
  );

  orleansTest.gap(
    "GAP-BUG-GRAIN-REF-IDENTITY",
    "DefaultCluster.Tests.General.MultifacetGrainTest.MultifacetFactory",
  );

  orleansTest.gap(
    "GAP-BUG-GRAIN-REF-IDENTITY",
    "DefaultCluster.Tests.General.MultifacetGrainTest.Multifacet_InterfacesAsArguments",
  );
});
