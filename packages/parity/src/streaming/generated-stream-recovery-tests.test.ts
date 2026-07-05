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
// per-stream event counts across all silos. This framework has: no
// persistent/queue-adapter stream provider with per-event synthetic error
// injection, no management-grain control-command channel into a running
// provider (tracked separately as GAP-MGMT-GRAIN), and no equivalent
// generated-event reporting grain. Reproducing the scenario would mean
// inventing the entire generator/queue-mapper/control-command stack, not
// porting an existing one.
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
