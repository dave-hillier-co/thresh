// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/ControllableStreamGeneratorProviderTests.cs @ v10.1.0 (MIT).
//
// Both tests configure the `GeneratorAdapterFactory` test-only queue adapter
// live, mid-test, via `IManagementGrain.SendControlCommandToProvider` (see
// `generated-stream-recovery-tests.test.ts`), then poll a dedicated
// `IGeneratedEventReporterGrain` that tallies per-stream event counts across
// every queue in a `HashRingStreamQueueMapper`-partitioned persistent stream
// provider.
//
// The base generator/queue-mapper/reporter stack now exists —
// `GeneratorPullingStreamProvider` + `GeneratorStreamQueue`
// (`packages/streams/src/generator-pulling-stream-provider.ts`,
// `generator-stream-queue.ts`, with a `reconfigure()` method standing in for
// `IControllable.ExecuteCommand`) and a reporter-grain fixture, ported for
// `stream-generator-provider-tests.test.ts`'s `ValidateGeneratedStreamsTest`.
// What these two tests still need is the *reach path* to that
// reconfiguration: upstream drives it live, mid-test, through
// `IManagementGrain.SendControlCommandToProvider` — a silo-control system
// target with no equivalent here (`GAP-MGMT-GRAIN`: no management grain /
// silo-control system targets at all). Without it there is no in-test way to
// invoke `reconfigure()` the way the upstream `[Fact]`s do, so both stay
// gapped under GAP-STREAM-GENERATOR-ADAPTER pending GAP-MGMT-GRAIN.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const NS = "UnitTests.StreamingTests.ControllableStreamGeneratorProviderTests";

orleansTest.gap("GAP-STREAM-GENERATOR-ADAPTER", `${NS}.ValidateControllableGeneratedStreamsTest`);
orleansTest.gap("GAP-STREAM-GENERATOR-ADAPTER", `${NS}.Validate2ControllableGeneratedStreamsTest`);

// vitest requires at least one runtime test per file; both upstream Facts are
// gaps above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
