// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/ControllableStreamGeneratorProviderTests.cs @ v10.1.0 (MIT).
//
// Both tests configure the `GeneratorAdapterFactory` test-only queue adapter
// live, mid-test, via `IManagementGrain.SendControlCommandToProvider` (see
// `generated-stream-recovery-tests.test.ts`), then poll a dedicated
// `IGeneratedEventReporterGrain` that tallies per-stream event counts across
// every queue in a `HashRingStreamQueueMapper`-partitioned persistent stream
// provider. This framework has no persistent/queue-adapter stream provider,
// no generator adapter, no hash-ring queue mapper, no management-grain
// control-command channel (`GAP-MGMT-GRAIN`), and no generated-event
// reporting grain — the same generator/queue-mapper/control-command stack
// gapped there under `GAP-STREAM-GENERATOR-ADAPTER`.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const NS = "UnitTests.StreamingTests.ControllableStreamGeneratorProviderTests";

orleansTest.gap("GAP-STREAM-GENERATOR-ADAPTER", `${NS}.ValidateControllableGeneratedStreamsTest`);
orleansTest.gap("GAP-STREAM-GENERATOR-ADAPTER", `${NS}.Validate2ControllableGeneratedStreamsTest`);

// vitest requires at least one runtime test per file; both upstream Facts are
// gaps above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
