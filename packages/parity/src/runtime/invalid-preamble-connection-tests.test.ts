// Ported from dotnet/orleans test/Orleans.Runtime.Tests/ClientConnectionTests/InvalidPreambleConnectionTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.ClientConnectionTests.InvalidPreambleConnectionTests", () => {
  // Opens a raw TCP socket to the gateway endpoint and sends an oversized
  // connection-preamble length prefix, asserting the server closes the
  // connection. This exercises Orleans' bespoke length-prefixed TCP wire
  // preamble (`ConnectionPreambleHelper`, `MaxPreambleLength`) — this
  // framework's transport is WebSocket-over-HTTP behind an abstraction with
  // no equivalent raw-socket preamble handshake (see docs/deviations.md).
  orleansTest.excluded(
    "Raw TCP socket wire-preamble handshake (ConnectionPreambleHelper/MaxPreambleLength) — this framework's transport is WebSocket-over-HTTP with no equivalent preamble handshake",
    "Tester.ClientConnectionTests.InvalidPreambleConnectionTests.ShouldCloseConnectionWhenClientSendsInvalidPreambleSize",
  );

  // This file's upstream Fact is entirely excluded (above); this placeholder
  // keeps the suite non-empty for the test runner and is not itself part of
  // the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
