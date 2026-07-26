// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/PersistentStreamPullingAgentTests.cs @ v10.1.0 (MIT).
//
// Every test white-box-tests Orleans' internal `PersistentStreamPullingAgent`
// system target directly — constructing it with NSubstitute mocks for
// `IStreamPubSub`/`IQueueAdapter`/`IQueueAdapterReceiver` and reaching into
// its private state via `PersistentStreamPullingAgent.ITestAccessor`
// (`ReadFromQueue`, `RegisterStream`, `GetPubSubCache`) to assert on internal
// registration-task tracking and pub/sub cache bookkeeping. This framework's
// streaming architecture has no pulling-agent system target at all —
// `MemoryStreamProvider` is a direct push-based fan-out with no queue
// adapter, no `IStreamPubSub`, and no per-stream registration-task cache to
// test-access. There is no equivalent internal component to port these
// mock-driven white-box tests to.
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const REASON =
  "white-box unit tests of Orleans' internal PersistentStreamPullingAgent system target (mocked IStreamPubSub/IQueueAdapter, ITestAccessor into private registration-task/pub-sub-cache state); this framework's MemoryStreamProvider has no pulling-agent system target, queue adapter, or pub/sub cache to test-access";

const NS = "UnitTests.StreamingTests.PersistentStreamPullingAgentTests";

orleansTest.excluded(REASON, `${NS}.ReadFromQueue_DoesNotWaitForColdStreamRegistration`);
orleansTest.excluded(
  REASON,
  `${NS}.ReadFromQueue_ClearsRegistrationTaskWhenColdStreamRegistrationCompletesSynchronously`,
);
orleansTest.excluded(
  REASON,
  `${NS}.RegisterStream_RemovesCacheEntryWhenProducerRegistrationTerminates`,
);
orleansTest.excluded(REASON, `${NS}.RegisterStream_KeepsCacheEntryWhenSubscriberHandshakeFails`);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
