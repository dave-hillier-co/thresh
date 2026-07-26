// Ported from dotnet/orleans test/Orleans.Core.Tests/Caching/ConcurrentLruSoakTests.cs @ v10.1.0 (MIT).
import { describe, it } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";

describe("NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests", () => {
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenSoakConcurrentGetCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenSoakConcurrentGetWithArgCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenSoakConcurrentGetAndRemoveCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenSoakConcurrentGetAndRemoveKvpCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenSoakConcurrentGetAndUpdateCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenSoakConcurrentGetAndAddCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenSoakConcurrentGetAndUpdateValueTypeCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenAddingCacheSizeItemsNothingIsEvicted",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenConcurrentUpdateAndRemoveKvp",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenConcurrentGetAndClearCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenConcurrentGetAndClearDuringWarmupCacheEndsInConsistentState",
  );
  orleansTest.excluded(
    "tests Orleans' internal FastConcurrentLru cache data structure, a .NET-specific implementation detail with no public equivalent in this framework",
    "NonSilo.Tests.Caching.ConcurrentLruCacheSoakTests.WhenValueIsBigStructNoLiveLock",
  );

  // vitest requires at least one runtime test per file; all upstream Facts are
  // excluded above, so this placeholder keeps the file a valid suite.
  it.skip("(all tests in this file are orleansTest.excluded - see above)", () => undefined);
});
