// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ClientConnectionTests/StallConnectionTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.ClientConnectionTests.StallConnectionTests", () => {
  // Both tests open a raw TCP socket to a gateway/silo endpoint and hold it
  // open without completing the connection handshake (a "stalled"
  // connection), then assert a real client/silo can still connect within
  // `ConnectionOptions.OpenConnectionTimeout`. Raw-socket stalling and
  // per-connection open timeouts are socket-transport internals this
  // framework's WebSocket-over-HTTP abstraction does not expose (see
  // docs/deviations.md), and configuration is via DI/hosting builders
  // (ISiloConfigurator, IClientBuilderConfigurator).
  const reason =
    "Raw-socket connection stalling + ConnectionOptions.OpenConnectionTimeout — socket-transport internals with no equivalent in this framework's WebSocket-over-HTTP transport, configured via DI/hosting builders";

  orleansTest.excluded(
    reason,
    "Tester.ClientConnectionTests.StallConnectionTests.ConnectToGwAfterStallConnectionOpened",
  );
  orleansTest.excluded(
    reason,
    "Tester.ClientConnectionTests.StallConnectionTests.SiloJoinAfterStallConnectionOpened",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
