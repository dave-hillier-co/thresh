// Ported from dotnet/orleans test/Orleans.Core.Tests/General/UtilsTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// Tests IPEndPoint/SiloAddress.ToGatewayUri(), which formats a "gwy.tcp://" URI for Orleans'
// TCP gateway transport. This framework routes client-to-silo traffic through Kubernetes
// services rather than a gateway URI scheme, and SiloAddress carries a pod identity, not an
// IPEndPoint, so there is no equivalent URI-formatting function to test.
describe("UnitTests.UtilsTests.UtilsTests", () => {
  orleansTest.excluded(
    ".NET-specific: formats Orleans' gwy.tcp:// gateway URI from an IPEndPoint/SiloAddress; this framework has no gateway-URI transport and SiloAddress is a Kubernetes pod identity, not an IPEndPoint",
    "UnitTests.UtilsTests.UtilsTests.ToGatewayUriTest",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
