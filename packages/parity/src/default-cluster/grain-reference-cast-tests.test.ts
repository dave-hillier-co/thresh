// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/GrainReferenceCastTests.cs @ v10.1.0 (MIT).
//
// Every test in this file turns on `GrainReference.AsReference<T>()` /
// `IInternalGrainFactory.Cast(grain, type)`: casting an already-obtained grain
// reference to a *different* grain interface at runtime, with the cast
// validated (optimistically before resolution, strictly after) against the
// interfaces the concrete grain type actually implements. This framework's
// grain references (`packages/runtime/src/grain-factory.ts` `buildProxy`) are
// built once, for one fixed `GrainInterface<T>` token, and expose no reference
// re-typing operation at all — there is no `AsReference`/`Cast` API on a grain
// proxy to port these onto. Introducing one is a real feature addition, not a
// simplification of an existing one, so every case here is a gap rather than
// an exclusion.
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.GrainReferenceCastTests", () => {
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefCastFromMyType",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefCastFromMyTypePolymorphic",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastMultifacetRWReference",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastMultifacetRWReferenceWaitForResolve",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastFailInternalCastFromBadType",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastInternalCastFromMyType",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastInternalCastUpFromChild",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefUpCastFromChild",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.FailSideCastAfterResolve",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.FailOperationAfterSideCast",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.FailSideCastAfterContinueWith",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefUpCastFromGrandchild",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastGrainRefUpCastFromDerivedDerivedChild",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastAsyncGrainRefCastFromSelf",
  );
  orleansTest.gap(
    "GAP-GRAIN-REF-CAST",
    "DefaultCluster.Tests.GrainReferenceCastTests.CastCallMethodInheritedFromBaseClass",
  );
});
