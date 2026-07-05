// Ported from dotnet/orleans test/Orleans.DefaultCluster.Tests/RequestContextTest.cs @ v10.1.0 (MIT).
// Upstream's namespace is literally `UnitTDefaultCluster.Tests.General` (a typo in the
// original source, preserved here so the id traces back exactly).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("UnitTDefaultCluster.Tests.General.RequestContextTests", () => {
  // Upstream sets `RequestContext.Set(...)` directly from the test method (i.e. from a
  // non-grain "client" caller) before invoking the grain, then reads `RequestContext.Keys`
  // back afterwards on the same ambient (.NET AsyncLocal) context. This framework's ambient
  // request context (`@tsva/runtime/invocation-context`) only exists inside a grain
  // activation's turn — there is no client-side ambient RequestContext a non-grain caller
  // can set before a call, and `packages/parity` has no dependency on `@tsva/runtime` to
  // reach into it directly even as a test-only workaround.
  orleansTest.gap(
    "GAP-CLIENT-REQUEST-CONTEXT",
    "UnitTDefaultCluster.Tests.General.RequestContextTests.RequestContextCallerToCalleeFlow",
  );

  orleansTest.excluded(
    'skipped upstream: [Fact(Skip = "Was failing before (just masked as a Pass), needs fixing or removing")]',
    "UnitTDefaultCluster.Tests.General.RequestContextTests.RequestContextCalleeToCallerFlow",
  );
});
