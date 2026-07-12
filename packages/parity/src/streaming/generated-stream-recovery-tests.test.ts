// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/GeneratedStreamRecoveryTests.cs @ v10.1.0 (MIT).
// Runner: test/Orleans.Streaming.Tests/StreamingTests/ImplicitSubscritionRecoverableStreamTestRunner.cs @ v10.1.0 (MIT).
//
// Both [Fact]s drive a persistent stream provider backed by
// `GeneratorAdapterFactory` — a test-only queue adapter that synthesizes
// events per stream, injecting transient and non-transient per-event errors,
// multiplexed over a `HashRingStreamQueueMapper` with a configurable queue
// count. Configuration happens live, mid-test, via
// `IManagementGrain.SendControlCommandToProvider` (a silo-control system
// target reaching into the running stream provider) and results are read
// back through a dedicated `IGeneratedEventReporterGrain` that tallies
// per-stream event counts across all silos.
//
// The base generator/queue-mapper/reporter stack now exists —
// `GeneratorPullingStreamProvider` + `GeneratorStreamQueue`
// (`packages/streams/src/generator-pulling-stream-provider.ts`,
// `generator-stream-queue.ts`) and a reporter-grain fixture, ported for
// `stream-generator-provider-tests.test.ts`'s `ValidateGeneratedStreamsTest`;
// and `IManagementGrain.SendControlCommandToProvider` — the live-reconfigure
// reach path these tests use — is now implemented too (see
// `controllable-stream-generator-provider-tests.test.ts`). What remains, and
// what makes these two the hardest streaming item left, is a per-subscription
// RECOVERABLE-CURSOR subsystem: the fault-injecting collectors
// (`ImplicitSubscription_{Transient,NonTransient}Error_RecoverableStream_CollectorGrain`)
// persist a `StreamCheckpoint<int>` (recovery token + accumulator, checkpointed
// every 10 events) and, on an injected fault, deactivate and later resume
// delivery from their stored recovery token — so a fault on e.g. the 33rd event
// must rewind delivery to the consumer's last checkpoint (event 30), not merely
// redeliver the one uncommitted event the pulling agent held. That needs
// resume-from-`StreamSequenceToken` with rewindable, per-subscription cursors
// (transient faults recover to a full 100/stream; one non-transient fault is
// permanently skipped, 99/stream) — a genuine cursor/checkpoint subsystem this
// push/pulling-agent model does not yet have, deferred to a dedicated
// streams-recovery slice rather than forced green.
import { orleansTest } from "@tsva/testing/orleans-test";

// UnitTests.StreamingTests.GeneratedImplicitSubscriptionStreamRecoveryTests
orleansTest.gap(
  "GAP-STREAM-GENERATOR-ADAPTER",
  "UnitTests.StreamingTests.GeneratedImplicitSubscriptionStreamRecoveryTests.Recoverable100EventStreamsWithTransientErrorsTest",
);

orleansTest.gap(
  "GAP-STREAM-GENERATOR-ADAPTER",
  "UnitTests.StreamingTests.GeneratedImplicitSubscriptionStreamRecoveryTests.Recoverable100EventStreamsWith1NonTransientErrorTest",
);
