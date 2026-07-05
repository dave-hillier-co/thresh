// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/StreamGeneratorProviderTests.cs @ v10.1.0 (MIT).
//
// Configures the `GeneratorAdapterFactory` test-only queue adapter up front
// (via a keyed `IStreamGeneratorConfig`), partitioned across a
// `HashRingStreamQueueMapper`, and polls a dedicated
// `IGeneratedEventReporterGrain` to confirm every queue produced its
// configured event count. Same generator/queue-mapper stack gapped in
// `generated-stream-recovery-tests.test.ts` and
// `controllable-stream-generator-provider-tests.test.ts` under
// `GAP-STREAM-GENERATOR-ADAPTER`: this framework has no persistent/
// queue-adapter stream provider, no generator adapter, no hash-ring queue
// mapper, and no generated-event reporting grain.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-STREAM-GENERATOR-ADAPTER",
  "UnitTests.StreamingTests.StreamGeneratorProviderTests.ValidateGeneratedStreamsTest",
);

// vitest requires at least one runtime test per file; the sole upstream Fact
// is a gap above, so this placeholder keeps the file a valid suite.
it.skip("(the sole test in this file is orleansTest.gap — see above)", () => undefined);
