// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryStreamProviderClientTests.cs @ v10.1.0 (MIT).
//
// Both tests in this file exercise a .NET-specific mechanism: an Orleans
// client is a process distinct from any silo, joins cluster membership, and
// can be hard-killed and reconnected independently
// (`TestCluster.KillClientAsync` / `InitializeClientAsync`), after which the
// silo-side `SiloMessagingOptions.ClientDropTimeout` reaps its stream
// producers/consumers. This framework has no separate client process or
// client membership entry — `TestCluster.getGrain` calls straight through a
// silo — so there is nothing here to hard-kill or reconnect.
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// Tester.StreamingTests.MemoryStreamProviderClientTests
orleansTest.excluded(
  ".NET client-process membership/reconnect protocol (TestCluster.KillClientAsync/InitializeClientAsync); this framework has no separate client process with cluster membership to drop",
  "Tester.StreamingTests.MemoryStreamProviderClientTests.MemoryStreamProducerOnDroppedClientTest",
);

orleansTest.excluded(
  ".NET client-process membership/reconnect protocol (TestCluster.KillClientAsync/InitializeClientAsync); this framework has no separate client process with cluster membership to drop",
  "Tester.StreamingTests.MemoryStreamProviderClientTests.MemoryStreamConsumerOnDroppedClientTest",
);

// vitest requires at least one runtime test per file; both upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
