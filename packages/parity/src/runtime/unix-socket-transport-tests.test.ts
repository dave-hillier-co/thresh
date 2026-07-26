// Ported from dotnet/orleans test/Orleans.Runtime.Tests/TransportTests/UnixSocketTransportTests.cs @ v10.1.0 (MIT).
// `UnixSocketTransportTests` itself declares no [Fact]s; both cases
// (`SimplePing`, `ProxyPing`) are inherited from `TransportTestsBase`
// (test/Orleans.Runtime.Tests/TransportTests/TransportTestsBase.cs) and run
// here under `ConnectionTransportType.UnixSocket`.
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("Tester.TransportTests.UnixSocketTransportTests", () => {
  // Both inherited cases exist purely to exercise Orleans' Unix-domain-socket
  // transport option (`ConnectionTransportType.UnixSocket`), skipped upstream
  // entirely on platforms without `AF_UNIX` support. This framework's
  // transport is WebSocket-over-HTTP behind an abstraction (see
  // docs/deviations.md) — there is no alternate socket-family transport to
  // select between, so the underlying ping/proxy-ping behaviour they'd
  // otherwise share with the default transport is already covered by
  // default-cluster generic-grain-tests rather than needing a duplicate here.
  const reason =
    "Unix-domain-socket transport selection (ConnectionTransportType.UnixSocket) — this framework's transport is WebSocket-over-HTTP with no alternate socket-family transport to select";

  orleansTest.excluded(reason, "Tester.TransportTests.UnixSocketTransportTests.SimplePing");
  orleansTest.excluded(reason, "Tester.TransportTests.UnixSocketTransportTests.ProxyPing");

  // This file's upstream Facts are entirely excluded (above); this
  // placeholder keeps the suite non-empty for the test runner and is not
  // itself part of the upstream accounting.
  it.skip("(all cases in this file are excluded — see above)", () => undefined);
});
