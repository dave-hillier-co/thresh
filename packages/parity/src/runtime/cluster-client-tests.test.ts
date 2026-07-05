// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ClientConnectionTests/ClusterClientTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.ClientConnectionTests.ClusterClientTests", () => {
  // Builds a bare `HostBuilder().UseOrleansClient(...)` with a custom
  // `IGatewayListProvider` and `UseConnectionRetryFilter`, asserting that a
  // `SiloUnavailableException` triggers exactly one retry before the gateway
  // list is repopulated. DI/hosting-builder machinery (HostBuilder, custom
  // IGatewayListProvider, connection retry filter) has no equivalent surface
  // in this framework, whose gateway/membership discovery is Kubernetes-based
  // (see docs/deviations.md) rather than a pluggable .NET provider.
  orleansTest.excluded(
    "DI/hosting builders + pluggable IGatewayListProvider/connection-retry-filter — this framework's gateway discovery is Kubernetes-based, not a pluggable .NET provider",
    "Tester.ClientConnectionTests.ClusterClientTests.ConnectIsRetryableTest",
  );

  // This file's upstream Fact is entirely excluded (above); this placeholder
  // keeps the suite non-empty for the test runner and is not itself part of
  // the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
