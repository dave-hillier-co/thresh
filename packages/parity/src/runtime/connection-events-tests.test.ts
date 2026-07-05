// Ported from dotnet/orleans test/Orleans.Runtime.Tests/Diagnostics/ConnectionEventsTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("Tester.Diagnostics.ConnectionEventsTests", () => {
  // Both tests directly exercise `Orleans.Core.Diagnostics.ConnectionEvents`
  // — a static `IObservable<ConnectionEvent>` pub/sub surface that emits
  // low-level transport connection lifecycle events (`Connecting`,
  // `AcceptFailed`) keyed on `SiloAddress`/`EndPoint`. This is internal
  // wire-transport instrumentation for the socket-based connection layer;
  // this framework's transport is WebSocket-over-HTTP behind an abstraction
  // (see docs/deviations.md) with no equivalent per-connection
  // connecting/accept-failure event stream to observe.
  const reason =
    "Orleans.Core.Diagnostics.ConnectionEvents — static observable transport-connection-lifecycle event stream tied to the socket-based connection layer; this framework's WebSocket-over-HTTP transport abstraction has no equivalent event stream";

  orleansTest.excluded(
    reason,
    "Tester.Diagnostics.ConnectionEventsTests.ConnectionEvents_EmitConnecting_EmitsConnecting",
  );
  orleansTest.excluded(
    reason,
    "Tester.Diagnostics.ConnectionEventsTests.ConnectionEvents_EmitAcceptFailed_EmitsAcceptFailed",
  );

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
