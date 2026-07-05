// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/ImplicitSubscriptionKeyTypeGrainTests.cs @ v10.1.0 (MIT).
//
// This test publishes onto the memory stream provider and expects an
// implicitly-subscribed grain (declared with `@implicitStreamSubscription`,
// no explicit `subscribe()` call) to receive the event. In this framework,
// implicit-subscription delivery is only wired up for pulling-agent-backed
// stream providers (`SiloBuilder.pullingStreams` — e.g. Redis streams):
// `SiloBuilder.build()` calls `provider.setDeliver(...)` /
// `provider.setImplicitSubscribers(...)` only for those. `useMemoryStreams()`
// registers a plain `MemoryStreamProvider`, which implements `StreamProvider`
// but not `ActivationBoundStreamProvider` (no `bindActivation`), so it is
// never added to `pullingStreams` and a `publish()` on it never reaches a
// grain that only declares an implicit subscription — only grains that
// explicitly `subscribe()` themselves receive memory-stream events. There is
// therefore no way to make this test's `IImplicitSubscriptionLongKeyGrain`
// (implicit-only) receive the published `int` over `TestCluster`'s memory
// stream provider.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-STREAM-IMPLICIT-MEMORY",
  "UnitTests.StreamingTests.ImplicitSubscriptionKeyTypeGrainTests.LongKey",
);

// vitest requires at least one runtime test per file; the sole upstream Fact
// is a gap above, so this placeholder keeps the file a valid suite.
it.skip("(the sole test in this file is orleansTest.gap — see above)", () => undefined);
