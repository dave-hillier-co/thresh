// Ported from dotnet/orleans test/Orleans.Streaming.Tests/OrleansRuntime/Streams/StreamingEventsTests.cs @ v10.1.0 (MIT).
//
// The sole test subscribes an `IObserver<StreamingEvents.StreamingEvent>`
// directly to `StreamingEvents.AllEvents` — an internal `IObservable`
// diagnostic feed Orleans' pulling-agent queue balancer emits
// `BalancerChanged` events onto (`Orleans.Streaming.Diagnostics`) — and
// asserts on the emitted event's fields after calling
// `StreamingEvents.EmitQueueChange` with a fake `IStreamQueueBalancer`. This
// framework has no pulling-agent queue balancer, no queue-distribution-change
// diagnostics feed, and no `StreamingEvents` equivalent to subscribe to or
// emit through — `MemoryStreamProvider` has no queue balancing at all.
import { it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

orleansTest.excluded(
  "white-box unit test of Orleans' internal StreamingEvents diagnostics feed (Orleans.Streaming.Diagnostics), emitted by the persistent-stream pulling-agent's queue balancer; this framework has no pulling-agent queue balancer or diagnostics feed to test-access",
  "UnitTests.OrleansRuntime.Streams.StreamingEventsTests.StreamingEvents_EmitQueueChange_EmitsBalancerChangedOnly",
);

// vitest requires at least one runtime test per file; the sole upstream Fact
// is excluded above, so this placeholder keeps the file a valid suite.
it.skip("(the sole test in this file is orleansTest.excluded — see above)", () => undefined);
