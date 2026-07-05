// Ported from dotnet/orleans test/Orleans.Runtime.Tests/LocalhostSiloTests.cs @ v10.1.0 (MIT).
// (Upstream class is `Tester.LocalhostClusterTests`, despite the file name.)
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.LocalhostClusterTests", () => {
  // Both tests build raw `HostBuilder().UseOrleans(...)`/`UseOrleansClient(...)`
  // hosts wired with `UseLocalhostClustering(siloPort, gatewayPort)` — a
  // convenience clustering mode for local development that talks real TCP
  // ports on loopback, including (for the multi-silo case) opting into the
  // experimental distributed grain directory. This framework's cluster
  // membership/gateway discovery is Kubernetes-based rather than a
  // loopback-port clustering mode (see docs/deviations.md), and there is no
  // `HostBuilder`/`IHost` DI-hosting layer to configure — its equivalent
  // local-dev entry point is `TestCluster.start`, already exercised by every
  // other file in this package.
  const reason =
    "HostBuilder/IHost DI-hosting + UseLocalhostClustering (loopback-TCP-port clustering mode) — this framework's membership/gateway discovery is Kubernetes-based with no loopback-port clustering mode, and its local-dev entry point is TestCluster.start (already exercised throughout this package)";

  orleansTest.excluded(reason, "Tester.LocalhostClusterTests.LocalhostSiloTest");
  orleansTest.excluded(reason, "Tester.LocalhostClusterTests.LocalhostClusterTest");

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
