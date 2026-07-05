// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ClientConnectionTests/ClientConnectionEventTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.ClientConnectionEventTests", () => {
  // Both tests subscribe to `AddClusterConnectionLostHandler` /
  // `AddGatewayCountChangedHandler` on an `InProcessTestClusterBuilder`
  // client, then stop silos and assert the events fire with the expected
  // gateway counts. This framework has no client-side connection/gateway
  // event API (its client is a direct in-process handle to a Kubernetes-
  // resolved silo set, not a gateway-count-tracking .NET client runtime) —
  // DI/hosting-builder machinery throughout.
  const reason =
    "DI/hosting builders + no client-side cluster-connection-lost/gateway-count-changed event API — this framework's client has no gateway-count-tracking runtime to notify";

  orleansTest.excluded(
    reason,
    "Tester.ClientConnectionEventTests.EventSendWhenDisconnectedFromCluster",
  );
  orleansTest.excluded(
    reason,
    "Tester.ClientConnectionEventTests.GatewayChangedEventSentOnDisconnectAndReconnect",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
