// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryStreamProviderBatchedClientTests.cs @ v10.1.0 (MIT).
//
// Batched variant of MemoryStreamProviderClientTests, sharing the same
// ClientStreamTestRunner; see memory-stream-provider-client-tests.test.ts for
// why both tests are excluded (client-process membership/reconnect is a
// .NET-specific mechanism this framework does not model).
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

// Tester.StreamingTests.MemoryStreamProviderBatchedClientTests
orleansTest.excluded(
  ".NET client-process membership/reconnect protocol (TestCluster.KillClientAsync/InitializeClientAsync); this framework has no separate client process with cluster membership to drop",
  "Tester.StreamingTests.MemoryStreamProviderBatchedClientTests.BatchedMemoryStreamProducerOnDroppedClientTest",
);

orleansTest.excluded(
  ".NET client-process membership/reconnect protocol (TestCluster.KillClientAsync/InitializeClientAsync); this framework has no separate client process with cluster membership to drop",
  "Tester.StreamingTests.MemoryStreamProviderBatchedClientTests.BatchedMemoryStreamConsumerOnDroppedClientTest",
);

// vitest requires at least one runtime test per file; both upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
