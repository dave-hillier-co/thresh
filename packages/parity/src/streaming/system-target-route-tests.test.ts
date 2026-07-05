// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/SystemTargetRouteTests.cs @ v10.1.0 (MIT).
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.excluded(
  "skipped upstream (dotnet/orleans#4320)",
  "Tester.StreamingTests.SystemTargetRouteTests.PersistentStreamingOverSingleGatewayTest",
);

// vitest requires at least one runtime test per file; the sole upstream Fact
// is excluded above, so this placeholder keeps the file a valid suite.
it.skip("(the sole test in this file is orleansTest.excluded — see above)", () => undefined);
