// Ported from dotnet/orleans test/Orleans.Core.Tests/Caching/ConcurrentLruTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("NonSilo.Tests.Caching.ConcurrentLruTests", () => {
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenCapacityIsLessThan3CtorThrows",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenCapacityIs4HotHasCapacity1AndColdHasCapacity1",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenCapacityIs10HotHasCapacity1AndWarmHasCapacity8AndColdHasCapacity1",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.ConstructAddAndRetrieveWithDefaultCtorReturnsValue",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemIsAddedCountIsCorrect",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsAddedKeysContainsTheKeys",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsAddedGenericEnumerateContainsKvps",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsAddedEnumerateContainsKvps",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.FromColdWarmupFillsWarmQueue",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemExistsTryGetReturnsValueAndTrue",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemDoesNotExistTryGetReturnsNullAndFalse",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemIsAddedThenRetrievedMetricHitRatioIsHalf",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemIsAddedThenRetrievedTotalIs2",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenRefToMetricsIsCapturedResultIsCorrect",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyIsRequestedItIsCreatedAndCached",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyIsRequestedWithArgItIsCreatedAndCached",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenDifferentKeysAreRequestedValueIsCreatedForEach",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValuesAreNotReadAndMoreKeysRequestedThanCapacityCountDoesNotIncrease",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValuesAreReadAndMoreKeysRequestedThanCapacityCountIsBounded",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeysAreContinuouslyRequestedInTheOrderTheyAreAddedCountIsBounded",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeysAreContinuouslyRequestedInTheOrderTheyAreAddedCountIsBounded2",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValueIsNotTouchedAndExpiresFromHotValueIsBumpedToCold",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValueIsTouchedAndExpiresFromHotValueIsBumpedToWarm",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValueIsTouchedAndExpiresFromColdItIsBumpedToWarm",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValueIsNotTouchedAndExpiresFromColdItIsRemoved",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValueIsNotTouchedAndExpiresFromWarmValueIsBumpedToCold",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValueIsTouchedAndExpiresFromWarmValueIsBumpedBackIntoWarm",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValueExpiresItIsDisposed",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenAddingNullValueCanBeAddedAndRemoved",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenValuesAreEvictedEvictionMetricCountsEvicted",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyExistsTryRemoveRemovesItemAndReturnsTrue",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyExistsTryRemoveReturnsValue",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemIsRemovedItIsDisposed",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemRemovedFromHotDuringWarmupItIsEagerlyCycledOut",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemRemovedFromHotAfterWarmupItIsEagerlyCycledOut",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemRemovedFromWarmDuringWarmupItIsEagerlyCycledOut",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemRemovedFromWarmAfterWarmupItIsEagerlyCycledOut",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemRemovedFromColdAfterWarmupItIsEagerlyCycledOut",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyDoesNotExistTryRemoveReturnsFalse",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsAreRemovedTrimRemovesDeletedItemsFromQueues",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenRepeatedlyAddingAndRemovingSameValueLruRemainsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyExistsTryUpdateUpdatesValueAndReturnsTrue",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyExistsTryUpdateDisposesOldValue",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyDoesNotExistTryUpdateReturnsFalse",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyExistsTryUpdateIncrementsUpdateCount",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyDoesNotExistTryUpdateDoesNotIncrementCounter",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyDoesNotExistAddOrUpdateAddsNewItem",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyExistsAddOrUpdateUpdatesExistingItem",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyExistsAddOrUpdateGuidUpdatesExistingItem",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyExistsAddOrUpdateDisposesOldValue",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenKeyDoesNotExistAddOrUpdateMaintainsLruOrder",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenCacheIsEmptyClearIsNoOp",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsExistClearRemovesAllItems",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenCacheIsSize3ItemsExistAndItemsAccessedClearRemovesAllItems",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsExistAndItemsAccessedClearRemovesAllItems",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenWarmThenClearedIsWarmIsReset",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenWarmThenTrimIsWarmIsReset",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsAreDisposableClearDisposesItemsOnRemove",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenTrimCountIsZeroThrows",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenTrimCountIsMoreThanCapacityThrows",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenColdItemsExistTrimRemovesExpectedItemCount",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenHotAndWarmItemsExistTrimRemovesExpectedItemCount",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenHotItemsExistTrimRemovesExpectedItemCount",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenColdItemsAreTouchedTrimRemovesExpectedItemCount",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsExistAndItemsAccessedTrimRemovesAllItems",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsRemovedClearRemovesAllItems",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruTests.WhenItemsAreDisposableTrimDisposesItems",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
