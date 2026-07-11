// Ported from dotnet/orleans test/Orleans.Core.Tests/ObserverManagerTests.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";
import { ObserverManager } from "@tsva/core/observer-manager";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";

// Tests Orleans.Utilities.ObserverManager<TIdentity, TObserver>, a standalone reusable utility
// for managing a snapshot-based, time-expiring collection of subscribers (used internally by
// Orleans grains that implement pub/sub or client-callback fan-out). Ported to
// packages/core/src/observer-manager.ts. Only the multi-threaded stress test is excluded, since
// it exercises .NET thread-pool concurrency this framework's single-threaded event loop has no
// counterpart for.
describe("NonSilo.Tests.ObserverManagerTests", () => {
  orleansTest("NonSilo.Tests.ObserverManagerTests.CollectionModified", async () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);

    for (let i = 0; i < 10; i++) {
      observerManager.subscribe(i, i);
    }

    // Using predicate for an easier simulation of inserting item while enumerating.
    function predicate(observer: number): boolean {
      if (observer === 5) {
        observerManager.subscribe(11, 1);
      }
      return true;
    }

    async function notification(_observer: number): Promise<void> {
      return Promise.resolve();
    }

    await expect(observerManager.notify(notification, predicate)).resolves.toBeUndefined();
  });

  orleansTest(
    "NonSilo.Tests.ObserverManagerTests.SubscribeDuringNotify_ObserversAreUpdatedImmediately",
    async () => {
      const clock = new FakeTimeProvider();
      const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);
      const notifiedObservers: number[] = [];
      let newlyAddedObserverNotified = false;

      // Subscribe some initial observers.
      for (let i = 0; i < 5; i++) {
        observerManager.subscribe(i, i);
      }

      async function notification(observer: number): Promise<void> {
        notifiedObservers.push(observer);

        // Add a new observer during notification of the first observer.
        if (observer === 0) {
          observerManager.subscribe(100, 100);

          // Verify that the newly added observer is immediately visible in the collection.
          expect([...observerManager.observers.values()]).toContain(100);
        }

        // Check if we're processing the newly added observer.
        if (observer === 100) {
          newlyAddedObserverNotified = true;
        }

        await Promise.resolve();
      }

      await observerManager.notify(notification);

      // Only the original observers should have been notified, not the one added during notification.
      expect(notifiedObservers).toHaveLength(5);

      // The newly added observer was not notified.
      expect(notifiedObservers).not.toContain(100);
      expect(newlyAddedObserverNotified).toBe(false);
    },
  );

  orleansTest(
    "NonSilo.Tests.ObserverManagerTests.UnsubscribeDuringNotify_ObserversAreUpdatedImmediately",
    async () => {
      const clock = new FakeTimeProvider();
      const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);
      const notifiedObservers: number[] = [];
      let unsubscribedObserverNotified = false;

      // Subscribe some initial observers.
      for (let i = 0; i < 5; i++) {
        observerManager.subscribe(i, i);
      }

      async function notification(observer: number): Promise<void> {
        notifiedObservers.push(observer);

        // Unsubscribe an observer during notification of the first observer.
        if (observer === 0) {
          observerManager.unsubscribe(3);

          // Verify that the unsubscribed observer is immediately removed from the collection.
          expect([...observerManager.observers.values()]).not.toContain(3);
        }

        // Check if we're processing the unsubscribed observer.
        if (observer === 3) {
          unsubscribedObserverNotified = true;
        }

        await Promise.resolve();
      }

      await observerManager.notify(notification);

      // Even though we unsubscribed during iteration, the snapshot still contains the original observers.
      // All original observers were notified due to snapshot.
      expect(notifiedObservers).toHaveLength(5);
      // Observer 3 was notified because it was in the snapshot.
      expect(unsubscribedObserverNotified).toBe(true);
      // But it's no longer in the collection after notification completed.
      expect(observerManager.count).toBe(4);
    },
  );

  orleansTest(
    "NonSilo.Tests.ObserverManagerTests.SyncNotify_SubscribeAndUnsubscribe_WorksCorrectly",
    () => {
      const clock = new FakeTimeProvider();
      const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);
      const notifiedObservers: number[] = [];

      // Subscribe some initial observers.
      for (let i = 0; i < 5; i++) {
        observerManager.subscribe(i, i);
      }

      function notification(observer: number): void {
        notifiedObservers.push(observer);

        // Add new observers and remove existing ones during notification.
        if (observer === 0) {
          observerManager.subscribe(100, 100);
          observerManager.unsubscribe(3);
        }
      }

      observerManager.notifySync(notification);

      // All original observers were notified.
      expect(notifiedObservers).toHaveLength(5);
      // Observer 3 was notified because it was in the snapshot.
      expect(notifiedObservers).toContain(3);
      // New observer wasn't in the original snapshot.
      expect(notifiedObservers).not.toContain(100);
      // 5 original - 1 removed + 1 added = 5 total.
      expect(observerManager.count).toBe(5);
    },
  );

  orleansTest(
    "NonSilo.Tests.ObserverManagerTests.ExceptionInNotificationCallback_RemovesObserver",
    async () => {
      const clock = new FakeTimeProvider();
      const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);

      // Subscribe some observers.
      for (let i = 0; i < 5; i++) {
        observerManager.subscribe(i, i);
      }

      async function notification(observer: number): Promise<void> {
        // Throw exception for specific observer.
        if (observer === 2) {
          throw new Error("Test exception");
        }
        await Promise.resolve();
      }

      await observerManager.notify(notification);

      // One observer should be removed.
      expect(observerManager.count).toBe(4);
      // Observer 2 should be removed due to exception.
      expect([...observerManager.observers.values()]).not.toContain(2);
    },
  );

  orleansTest(
    "NonSilo.Tests.ObserverManagerTests.SyncNotify_ExceptionInNotificationCallback_RemovesObserver",
    () => {
      const clock = new FakeTimeProvider();
      const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);

      // Subscribe some observers.
      for (let i = 0; i < 5; i++) {
        observerManager.subscribe(i, i);
      }

      function notification(observer: number): void {
        // Throw exception for specific observer.
        if (observer === 2) {
          throw new Error("Test exception");
        }
      }

      observerManager.notifySync(notification);

      // One observer should be removed.
      expect(observerManager.count).toBe(4);
      // Observer 2 should be removed due to exception.
      expect([...observerManager.observers.values()]).not.toContain(2);
    },
  );

  orleansTest("NonSilo.Tests.ObserverManagerTests.ExpiredObservers_AreRemovedDuringNotify", async () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ minutes: 30 }, clock);
    const notifiedObservers: number[] = [];

    // Subscribe some observers.
    for (let i = 0; i < 5; i++) {
      observerManager.subscribe(i, i);
    }

    // Move some observers into the past to make them expire.
    // Advance time by 1 hour.
    clock.advance(60 * 60 * 1000);

    async function notification(observer: number): Promise<void> {
      notifiedObservers.push(observer);
      await Promise.resolve();
    }

    await observerManager.notify(notification);

    // No observers should be notified as all are expired.
    expect(notifiedObservers).toHaveLength(0);
    // All observers should be removed due to expiration.
    expect(observerManager.count).toBe(0);
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.ClearExpired_RemovesOnlyExpiredObservers", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ minutes: 30 }, clock);

    // Subscribe some observers.
    for (let i = 0; i < 10; i++) {
      observerManager.subscribe(i, i);
    }

    // Update last seen time for half of the observers.
    clock.advance(25 * 60 * 1000);

    for (let i = 5; i < 10; i++) {
      // This refreshes the last-seen time.
      observerManager.subscribe(i, i);
    }

    // Advance time to expire only the first half (35 minutes from start).
    clock.advance(10 * 60 * 1000);

    observerManager.clearExpired();

    // Half should be removed due to expiration.
    expect(observerManager.count).toBe(5);
    for (let i = 0; i < 5; i++) {
      // First half should be removed.
      expect([...observerManager.observers.values()]).not.toContain(i);
    }

    for (let i = 5; i < 10; i++) {
      // Second half should remain.
      expect([...observerManager.observers.values()]).toContain(i);
    }
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.Clear_RemovesAllObservers", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);

    // Subscribe some observers.
    for (let i = 0; i < 10; i++) {
      observerManager.subscribe(i, i);
    }

    // Verify initial count.
    expect(observerManager.count).toBe(10);

    observerManager.clear();

    expect(observerManager.count).toBe(0);
    expect(observerManager.observers.size).toBe(0);
  });

  orleansTest.excluded(
    ".NET-specific: stress-tests ObserverManager's thread-safety across concurrent Task.Factory.StartNew work on a ConcurrentExclusiveSchedulerPair; this framework has no equivalent observer-management primitive to race, and no multi-threaded scheduler",
    "NonSilo.Tests.ObserverManagerTests.CanModifyObserversConcurrently",
  );

  orleansTest("NonSilo.Tests.ObserverManagerTests.PredicateFiltersObserversToNotify", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);
    const notifiedObservers: number[] = [];

    // Subscribe some observers.
    for (let i = 0; i < 10; i++) {
      observerManager.subscribe(i, i);
    }

    function evenNumberPredicate(observer: number): boolean {
      return observer % 2 === 0;
    }

    function notification(observer: number): void {
      notifiedObservers.push(observer);
    }

    observerManager.notifySync(notification, evenNumberPredicate);

    // Only even observers should be notified.
    expect(notifiedObservers).toHaveLength(5);
    // All notified observers should be even.
    for (const observer of notifiedObservers) {
      expect(observer % 2).toBe(0);
    }
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.AsyncPredicateFiltersObserversToNotify", async () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);
    const notifiedObservers: number[] = [];

    // Subscribe some observers.
    for (let i = 0; i < 10; i++) {
      observerManager.subscribe(i, i);
    }

    function oddNumberPredicate(observer: number): boolean {
      return observer % 2 === 1;
    }

    async function notification(observer: number): Promise<void> {
      notifiedObservers.push(observer);
      await Promise.resolve();
    }

    await observerManager.notify(notification, oddNumberPredicate);

    // Only odd observers should be notified.
    expect(notifiedObservers).toHaveLength(5);
    // All notified observers should be odd.
    for (const observer of notifiedObservers) {
      expect(observer % 2).toBe(1);
    }
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.EnumeratorWorksCorrectly", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);

    // Subscribe some observers.
    for (let i = 0; i < 5; i++) {
      observerManager.subscribe(i, i);
    }

    const enumeratedObservers: number[] = [];
    for (const observer of observerManager) {
      enumeratedObservers.push(observer);
    }

    expect(enumeratedObservers).toHaveLength(5);
    expect([...enumeratedObservers].sort((a, b) => a - b)).toEqual([0, 1, 2, 3, 4]);
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.SubscribingExistingObserver_UpdatesLastSeenTime", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ minutes: 30 }, clock);

    // Subscribe initial observer.
    observerManager.subscribe(1, 42);

    // Advance time to near expiration but not quite.
    clock.advance(25 * 60 * 1000);

    // Update the subscription (same ID, same observer value).
    observerManager.subscribe(1, 42);

    // Advance time to past the original subscription time + expiration (35 min from start).
    clock.advance(10 * 60 * 1000);

    let notified = false;
    observerManager.notifySync(() => {
      notified = true;
    });

    // Observer should still be active due to the refresh.
    expect(notified).toBe(true);
    expect(observerManager.count).toBe(1);
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.SubscribingExistingObserver_UpdatesObserverValue", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);

    // Subscribe initial observer.
    observerManager.subscribe(1, 100);

    // Update the subscription (same ID, different observer value).
    observerManager.subscribe(1, 200);

    let observedValue = 0;
    observerManager.notifySync((observer) => {
      observedValue = observer;
    });

    // Should get the updated value.
    expect(observedValue).toBe(200);
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.SetExpirationDuration_AffectsExpiration", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);

    // Subscribe some observers.
    for (let i = 0; i < 5; i++) {
      observerManager.subscribe(i, i);
    }

    // Change expiration to a shorter duration after subscribing.
    observerManager.expirationDuration = { minutes: 30 };

    // Advance time to between the old and new expiration durations.
    // Past 30min, but before 1hr.
    clock.advance(45 * 60 * 1000);

    const notifiedObservers: number[] = [];
    observerManager.notifySync((observer) => {
      notifiedObservers.push(observer);
    });

    // No observers should be notified due to expiration.
    expect(notifiedObservers).toHaveLength(0);
    // All observers should be removed.
    expect(observerManager.count).toBe(0);
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.ClearDuringNotification_WorksCorrectly", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);
    const notifiedObservers: number[] = [];

    // Subscribe some observers.
    for (let i = 0; i < 10; i++) {
      observerManager.subscribe(i, i);
    }

    // Clear the collection from within the first notification callback, mirroring the upstream
    // test's intent (clearing mid-notification) without requiring real concurrency: this
    // framework's notify walks a snapshot synchronously, so clearing from inside the callback
    // for the first observer has the same observable effect as clearing concurrently.
    observerManager.notifySync((observer) => {
      notifiedObservers.push(observer);
      if (observer === 0) {
        observerManager.clear();
      }
    });

    // We should still have notified all the original observers in the snapshot.
    expect(notifiedObservers).toHaveLength(10);

    // But the collection should now be empty.
    expect(observerManager.count).toBe(0);
    expect(observerManager.observers.size).toBe(0);
  });

  orleansTest("NonSilo.Tests.ObserverManagerTests.ModifyDuringEnumeration_WorksWithoutExceptions", () => {
    const clock = new FakeTimeProvider();
    const observerManager = new ObserverManager<number, number>({ hours: 1 }, clock);

    // Subscribe some initial observers.
    for (let i = 0; i < 5; i++) {
      observerManager.subscribe(i, i);
    }

    const enumeratedObservers: number[] = [];
    const newObserversAdded: number[] = [];
    let removedObserver = -1;

    // Enumerate using for-of, which uses [Symbol.iterator] under the hood.
    for (const observer of observerManager) {
      enumeratedObservers.push(observer);

      // Add and remove observers during enumeration.
      if (observer === 2) {
        // Add new observers.
        for (let i = 10; i < 15; i++) {
          observerManager.subscribe(i, i);
          newObserversAdded.push(i);
        }

        // Remove an observer.
        observerManager.unsubscribe(4);
        removedObserver = 4;

        // Verify immediate visibility in the collection (but not in the enumeration).
        expect([...observerManager.observers.values()]).not.toContain(4);
        for (const newObserver of newObserversAdded) {
          expect([...observerManager.observers.values()]).toContain(newObserver);
        }
      }
    }

    // Original enumeration should complete with all original observers.
    expect(enumeratedObservers).toHaveLength(5);
    // Even though we removed it during enumeration.
    expect(enumeratedObservers).toContain(removedObserver);

    // Verify final collection state.
    // 5 original - 1 removed + 5 added = 9 total.
    expect(observerManager.count).toBe(9);
    expect([...observerManager]).not.toContain(removedObserver);
    for (const newObserver of newObserversAdded) {
      expect([...observerManager]).toContain(newObserver);
    }
  });
});
