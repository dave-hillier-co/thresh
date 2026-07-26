// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ClientConnectionTests/ClientConnectionRegisteredServiceEventTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("Tester.ClientConnectionRegisteredServiceEventTests", () => {
  // Same scenario as ClientConnectionEventTests.cs (cluster-connection-lost /
  // gateway-count-changed events), but wired through an
  // `IClientBuilderConfigurator` registering a DI singleton `EventNotifier` —
  // still DI/hosting-builder machinery plus the same missing client-side
  // event API.
  const reason =
    "DI/hosting builders (IClientBuilderConfigurator, DI-registered EventNotifier singleton) + no client-side cluster-connection-lost/gateway-count-changed event API in this framework";

  orleansTest.excluded(
    reason,
    "Tester.ClientConnectionRegisteredServiceEventTests.EventSendWhenDisconnectedFromCluster",
  );
  orleansTest.excluded(
    reason,
    "Tester.ClientConnectionRegisteredServiceEventTests.GatewayChangedEventSentOnDisconnectAndReconnect",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
