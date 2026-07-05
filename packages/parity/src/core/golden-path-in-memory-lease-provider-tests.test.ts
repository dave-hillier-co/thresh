// Ported from dotnet/orleans test/Orleans.Core.Tests/Lease/GoldenPathInMemoryLeaseProviderTests.cs @ v10.1.0 (MIT).
import { describe } from "vitest";
import { orleansTest } from "@tsva/testing/orleans-test";

describe("DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests", () => {
  orleansTest.gap(
    "GAP-LEASE-PROVIDER",
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.ProviderCanAcquireLeases",
  );
  orleansTest.gap(
    "GAP-LEASE-PROVIDER",
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.ProviderCanReleaseLeases",
  );
  orleansTest.gap(
    "GAP-LEASE-PROVIDER",
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.ProviderCanRenewLeases",
  );
  orleansTest.gap(
    "GAP-LEASE-PROVIDER",
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.Provider_TryAcquireLeaseWhichBelongToOtherEntity_Return_LeaseNotAvailable",
  );
  orleansTest.gap(
    "GAP-LEASE-PROVIDER",
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.Provider_TryRenewLeaseWithWrongToken_Return_InvalidToken",
  );
  orleansTest.gap(
    "GAP-LEASE-PROVIDER",
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.Provider_LifeCycle_Acqurie_Renew_Release_ShouldBeAbleToAcquireLeaseOnTheSameResourceAfterRelease",
  );
});
