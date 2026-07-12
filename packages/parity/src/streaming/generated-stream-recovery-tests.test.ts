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
// `stream-generator-provider-tests.test.ts`'s `ValidateGeneratedStreamsTest`
// — but these two tests additionally need: (1) per-event synthetic error
// injection (transient vs. non-transient) into the generator, which the
// ported generator does not model; and (2) live mid-test reconfiguration
// reached through a management grain (`IManagementGrain.SendControlCommandToProvider`),
// which needs GAP-MGMT-GRAIN (no management grain / silo-control system
// targets in this framework at all). Both are additional surface area beyond
// the base generator adapter, so these stay gapped under
// GAP-STREAM-GENERATOR-ADAPTER pending GAP-MGMT-GRAIN plus generator-side
// error injection and queue-ownership recovery semantics.
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
