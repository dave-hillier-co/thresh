// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/MemoryProgrammaticSubcribeTests.cs @ v10.1.0 (MIT).
// Base class: test/Orleans.Streaming.Tests/StreamingTests/ProgrammaticSubscribeTests/ProgrammaticSubscribeTestsRunner.cs @ v10.1.0 (MIT).
//
// Every test method here is inherited from ProgrammaticSubscribeTestsRunner
// and runs, upstream, against the concrete MemoryProgrammaticSubcribeTests
// class — so ids use that concrete class's namespace, per this repo's
// convention for runner-derived suites (see memory-stream-batching-tests.test.ts).
//
// Upstream's `SubscriptionManager` helper wraps `IStreamSubscriptionManagerAdmin`
// / `IStreamSubscriptionManager` (`StreamSubscriptionManagerType.ExplicitSubscribeOnly`):
// it lets a test add/remove/list a subscription for an arbitrary grain
// reference *administratively*, without that grain ever calling `subscribe()`
// itself, and independently of any implicit-subscription declaration. This
// framework's stream model only supports a grain subscribing itself (from
// inside its own activation, via `this.runtime.getStreamProvider()`) or an
// implicit-subscription observer — there is no admin API to attach an
// arbitrary grain reference to a stream from outside, nor to enumerate/remove
// a stream's subscriptions administratively, nor an "explicit-subscribe-only"
// provider mode, nor `IStreamSubscriptionManagerAdmin.CanGetSubscriptionManager`.
// Upstream also synchronizes several of these tests on `StreamingDiagnosticObserver`,
// an internal pulling-agent diagnostic hook this framework has no equivalent
// of. So every test below is a GAP, except the two upstream already skips.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.gap(
  "GAP-STREAM-SUBSCRIPTION-MANAGER",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.Programmatic_Subscribe_Provider_WithExplicitPubsub_TryGetStreamSubscrptionManager",
);

orleansTest.gap(
  "GAP-STREAM-SUBSCRIPTION-MANAGER",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.Programmatic_Subscribe_CanUseNullNamespace",
);

orleansTest.gap(
  "GAP-STREAM-SUBSCRIPTION-MANAGER",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_Subscribe",
);

orleansTest.excluded(
  "skipped upstream (dotnet/orleans#5635)",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_UnSubscribe",
);

orleansTest.gap(
  "GAP-STREAM-SUBSCRIPTION-MANAGER",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_GetSubscriptions",
);

orleansTest.gap(
  "GAP-STREAM-SUBSCRIPTION-MANAGER",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_ConsumerUnsubscribeOnAdd",
);

orleansTest.excluded(
  "skipped upstream (dotnet/orleans#5650)",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_SubscribeToTwoStream_MessageWithPolymorphism",
);

orleansTest.gap(
  "GAP-STREAM-SUBSCRIPTION-MANAGER",
  "UnitTests.StreamingTests.MemoryProgrammaticSubcribeTests.StreamingTests_Consumer_Producer_SubscribeToStreamsHandledByDifferentStreamProvider",
);

// vitest requires at least one runtime test per file; all upstream Facts are
// gaps or excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap/.excluded — see above)", () => undefined);
