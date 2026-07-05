// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ClientConnectionTests/GatewayConnectionTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.GatewayConnectionTests", () => {
  // Both tests drive `OutsideRuntimeClient`/`ConnectionManager` directly
  // against raw TCP gateway sockets — one asserts a stale gateway is never
  // reconnected to once `IGatewayListProvider.GetGateways()` stops returning
  // it, the other asserts a client from a different cluster ID is rejected
  // at the wire preamble. Both require `ConnectionTransportType.TcpSocket`,
  // `UseTestClusterMembership = false`, and custom `IHostConfigurator`s —
  // socket-transport internals and DI/hosting builders this framework does
  // not expose (its transport is WebSocket-over-HTTP behind an abstraction,
  // and membership is Kubernetes-based; see docs/deviations.md).
  const reason =
    "Socket-transport internals (TcpSocket gateway connections, wire preamble/cluster-ID rejection) + DI/hosting builders — this framework's transport is WebSocket-over-HTTP and membership is Kubernetes-based";

  orleansTest.excluded(
    reason,
    "Tester.GatewayConnectionTests.NoReconnectionToGatewayNotReturnedByManager",
  );
  orleansTest.excluded(
    reason,
    "Tester.GatewayConnectionTests.ConnectionFromDifferentClusterIsRejected",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
