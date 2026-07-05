// Ported from dotnet/orleans test/Orleans.Core.Tests/ObserverManagerTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

// Tests Orleans.Utilities.ObserverManager<TIdentity, TObserver>, a standalone reusable utility
// for managing a snapshot-based, time-expiring collection of subscribers (used internally by
// Orleans grains that implement pub/sub or client-callback fan-out). This is a portable,
// non-.NET-specific algorithm (a map with TTL-based expiry and snapshot-before-notify
// semantics) that this framework simply has not built yet, so it is tracked as a gap under the
// existing GAP-OBSERVERS tag rather than excluded. Only the multi-threaded stress test is
// excluded, since it exercises .NET thread-pool concurrency this framework's single-threaded
// event loop has no counterpart for.
describe("NonSilo.Tests.ObserverManagerTests", () => {
  const reason = "GAP-OBSERVERS";

  orleansTest.gap(reason, "NonSilo.Tests.ObserverManagerTests.CollectionModified");
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.SubscribeDuringNotify_ObserversAreUpdatedImmediately",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.UnsubscribeDuringNotify_ObserversAreUpdatedImmediately",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.SyncNotify_SubscribeAndUnsubscribe_WorksCorrectly",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.ExceptionInNotificationCallback_RemovesObserver",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.SyncNotify_ExceptionInNotificationCallback_RemovesObserver",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.ExpiredObservers_AreRemovedDuringNotify",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.ClearExpired_RemovesOnlyExpiredObservers",
  );
  orleansTest.gap(reason, "NonSilo.Tests.ObserverManagerTests.Clear_RemovesAllObservers");
  orleansTest.excluded(
    ".NET-specific: stress-tests ObserverManager's thread-safety across concurrent Task.Factory.StartNew work on a ConcurrentExclusiveSchedulerPair; this framework has no equivalent observer-management primitive to race, and no multi-threaded scheduler",
    "NonSilo.Tests.ObserverManagerTests.CanModifyObserversConcurrently",
  );
  orleansTest.gap(reason, "NonSilo.Tests.ObserverManagerTests.PredicateFiltersObserversToNotify");
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.AsyncPredicateFiltersObserversToNotify",
  );
  orleansTest.gap(reason, "NonSilo.Tests.ObserverManagerTests.EnumeratorWorksCorrectly");
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.SubscribingExistingObserver_UpdatesLastSeenTime",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.SubscribingExistingObserver_UpdatesObserverValue",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.SetExpirationDuration_AffectsExpiration",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.ClearDuringNotification_WorksCorrectly",
  );
  orleansTest.gap(
    reason,
    "NonSilo.Tests.ObserverManagerTests.ModifyDuringEnumeration_WorksWithoutExceptions",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // gapped/excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.gap/excluded - see above)", () => undefined);
});
