// Ported from dotnet/orleans test/Orleans.Streaming.Tests/StreamingTests/BroadcastChannels/BroadcastChannelTests.cs @ v10.1.0 (MIT).
//
// Every test in this file publishes from a `_fixture.Client`-side
// `IBroadcastChannelProvider` obtained via `Client.GetBroadcastChannelProvider`,
// synchronizes on a `BroadcastChannelDiagnosticObserver` (an internal delivery-
// count hook), and asserts on two named providers configured with different
// `FireAndForgetDelivery` settings plus a regex-namespace subscriber grain
// (`IRegexNamespaceSubscriberGrain`) that also observes "multiple-namespaces-*".
//
// This framework's broadcast channels (`@tsva/core/broadcast-channel`,
// `BroadcastChannelProviderImpl`) differ in every one of those respects:
//   - `TestCluster` wires exactly one "default" provider (`useBroadcastChannels()`
//     in `test-cluster.ts`) and exposes no client-side writer — only a grain can
//     call `this.runtime.getBroadcastChannelProvider()`; there is no
//     `_fixture.Client.GetBroadcastChannelProvider(name)` equivalent to publish
//     from outside a grain.
//   - There is no `FireAndForgetDelivery` option and no second provider: fan-out
//     always awaits every subscriber via `Promise.all` in
//     `ClusterNode.publishToBroadcastChannel`, so a single failing subscriber
//     rejects the whole `publish()` call for every subscriber, unlike either of
//     Orleans' two modes (fire-and-forget swallows failures and reports counts
//     via the diagnostic observer; non-fire-and-forget aggregates them into one
//     `AggregateException` after every subscriber has been tried).
//   - There is no `BroadcastChannelDiagnosticObserver` (or any diagnostic hook) to
//     synchronize on "N deliveries have happened" without racing the publish.
//   - There is no regex-namespace channel-subscription predicate — only an exact
//     namespace string (`@implicitChannelSubscription`) — so there is no grain
//     equivalent to `IRegexNamespaceSubscriberGrain`.
// Reproducing these scenarios would mean inventing the client-side provider
// surface, a second configurable provider, per-subscriber failure isolation,
// a diagnostic observer, and a regex-namespace predicate — none of which exist.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

const NS = "Tester.StreamingTests.BroadcastChannel.BroadcastChannelTests";

orleansTest.gap("GAP-BROADCAST-CHANNEL-CLIENT", `${NS}.ClientPublishSingleChannelTest`);
orleansTest.gap(
  "GAP-BROADCAST-CHANNEL-CLIENT",
  `${NS}.ClientPublishSingleChannelMultipleConsumersTest`,
);
orleansTest.gap("GAP-BROADCAST-CHANNEL-CLIENT", `${NS}.ClientPublishMultipleChannelTest`);
orleansTest.gap("GAP-BROADCAST-CHANNEL-CLIENT", `${NS}.MultipleSubscribersOneBadActorChannelTest`);
orleansTest.gap(
  "GAP-BROADCAST-CHANNEL-CLIENT",
  `${NS}.NonFireAndForgetClientPublishSingleChannelTest`,
);
orleansTest.gap(
  "GAP-BROADCAST-CHANNEL-CLIENT",
  `${NS}.NonFireAndForgetClientPublishMultipleChannelTest`,
);
orleansTest.gap(
  "GAP-BROADCAST-CHANNEL-CLIENT",
  `${NS}.NonFireAndForgetClientPublishSingleChannelMultipleConsumersTest`,
);
orleansTest.gap(
  "GAP-BROADCAST-CHANNEL-CLIENT",
  `${NS}.NonFireAndForgetMultipleSubscribersOneBadActorChannelTest`,
);

// vitest requires at least one runtime test per file; all upstream Facts are
// gaps above, so this placeholder keeps the file a valid suite.
it.skip("(all tests in this file are orleansTest.gap — see above)", () => undefined);
