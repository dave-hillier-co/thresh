// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/ProgrammaticSubscribeTests/SubscriptionObserverWithImplicitSubscribingTestRunner.cs @ v10.1.0 (MIT).
//
// This runner is abstract and, within Orleans.Streaming.Tests (this suite's
// scope), has no concrete subclass — it is only driven by
// AdoNetSubscriptionObserverWithImplicitSubscribingTests,
// AQSubscriptionObserverWithImplicitSubscribingTests,
// RedisSubscriptionObserverWithImplicitSubscribingTests and
// EHSubscriptionObserverWithImplicitSubscribingTests, all in other test
// projects (Orleans.AdoNet.Tests / Orleans.Azure.Tests / Orleans.Redis.Tests /
// Orleans.Streaming.EventHubs.Tests) exercising DB/queue-backed streaming
// providers outside this assignment's scope. None of its four
// [SkippableFact] methods ever runs as part of Orleans.Streaming.Tests
// itself, so there is nothing to port here.
import { it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

const REASON =
  "abstract runner has no concrete subclass within Orleans.Streaming.Tests; only driven by out-of-scope AdoNet/Azure/Redis/EventHubs backend test projects";

orleansTest.excluded(
  REASON,
  "Tester.StreamingTests.ProgrammaticSubscribeTests.SubscriptionObserverWithImplicitSubscribingTestRunner.StreamingTests_ImplicitSubscribProvider_DontHaveSubscriptionManager",
);
orleansTest.excluded(
  REASON,
  "Tester.StreamingTests.ProgrammaticSubscribeTests.SubscriptionObserverWithImplicitSubscribingTestRunner.StreamingTests_Consumer_Producer_Subscribe",
);
orleansTest.excluded(
  REASON,
  "Tester.StreamingTests.ProgrammaticSubscribeTests.SubscriptionObserverWithImplicitSubscribingTestRunner.StreamingTests_Consumer_Producer_SubscribeToTwoStream_MessageWithPolymorphism",
);
orleansTest.excluded(
  REASON,
  "Tester.StreamingTests.ProgrammaticSubscribeTests.SubscriptionObserverWithImplicitSubscribingTestRunner.StreamingTests_Consumer_Producer_SubscribeToStreamsHandledByDifferentStreamProvider",
);

// vitest requires at least one runtime test per file; all upstream Facts are
// excluded above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.excluded — see above)", () => undefined);
