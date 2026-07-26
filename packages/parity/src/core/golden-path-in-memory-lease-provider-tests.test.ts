// Ported from dotnet/orleans test/Orleans.Core.Tests/Lease/GoldenPathInMemoryLeaseProviderTests.cs
// and test/TestInfrastructure/TestExtensions/Runners/GoldenPathLeaseProviderTestRunner.cs @ v10.1.0 (MIT).
import { describe, expect } from "vitest";
import { orleansTest } from "@thresh/testing/orleans-test";
import {
  InMemoryLeaseProvider,
  type AcquiredLease,
  type LeaseRequest,
} from "@thresh/core/lease-provider";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";

const LEASE_CATEGORY = "TestLeaseCategory";

function newResourceKey(): string {
  return crypto.randomUUID();
}

describe("DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests", () => {
  orleansTest(
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.ProviderCanAcquireLeases",
    async () => {
      const provider = new InMemoryLeaseProvider(new FakeTimeProvider());
      const leaseRequests: LeaseRequest[] = [
        { resourceKey: newResourceKey(), durationMs: 15_000 },
        { resourceKey: newResourceKey(), durationMs: 15_000 },
      ];

      const results = await provider.acquire(LEASE_CATEGORY, leaseRequests);

      results.forEach((result, i) => {
        expect(result.statusCode).toBe("OK");
        expect(result.acquiredLease).not.toBeUndefined();
        expect(result.acquiredLease?.resourceKey).toBe(leaseRequests[i]!.resourceKey);
      });
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.ProviderCanReleaseLeases",
    async () => {
      const provider = new InMemoryLeaseProvider(new FakeTimeProvider());
      const leaseRequests: LeaseRequest[] = [
        { resourceKey: newResourceKey(), durationMs: 15_000 },
        { resourceKey: newResourceKey(), durationMs: 15_000 },
      ];

      const results = await provider.acquire(LEASE_CATEGORY, leaseRequests);

      await expect(
        provider.release(
          LEASE_CATEGORY,
          results.map((result) => result.acquiredLease as AcquiredLease),
        ),
      ).resolves.toBeUndefined();
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.ProviderCanRenewLeases",
    async () => {
      const provider = new InMemoryLeaseProvider(new FakeTimeProvider());
      const leaseRequests: LeaseRequest[] = [
        { resourceKey: newResourceKey(), durationMs: 15_000 },
        { resourceKey: newResourceKey(), durationMs: 15_000 },
      ];

      const acquireResults = await provider.acquire(LEASE_CATEGORY, leaseRequests);
      const renewResults = await provider.renew(
        LEASE_CATEGORY,
        acquireResults.map((result) => result.acquiredLease as AcquiredLease),
      );

      renewResults.forEach((result, i) => {
        expect(result.statusCode).toBe("OK");
        expect(result.acquiredLease).not.toBeUndefined();
        expect(result.acquiredLease?.resourceKey).toBe(leaseRequests[i]!.resourceKey);
      });
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.Provider_TryAcquireLeaseWhichBelongToOtherEntity_Return_LeaseNotAvailable",
    async () => {
      const provider = new InMemoryLeaseProvider(new FakeTimeProvider());
      const resourceId = newResourceKey();
      const leaseRequests: LeaseRequest[] = [
        { resourceKey: resourceId, durationMs: 15_000 },
        { resourceKey: resourceId, durationMs: 15_000 },
      ];

      // Two entities try to acquire a lease on the same resource: one succeeds, one fails.
      const results = await provider.acquire(LEASE_CATEGORY, leaseRequests);

      expect(results.some((result) => result.statusCode === "OK")).toBe(true);
      expect(results.some((result) => result.statusCode === "LeaseNotAvailable")).toBe(true);
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.Provider_TryRenewLeaseWithWrongToken_Return_InvalidToken",
    async () => {
      const provider = new InMemoryLeaseProvider(new FakeTimeProvider());
      const leaseRequests: LeaseRequest[] = [
        { resourceKey: newResourceKey(), durationMs: 15_000 },
        { resourceKey: newResourceKey(), durationMs: 15_000 },
      ];

      const results = await provider.acquire(LEASE_CATEGORY, leaseRequests);
      const acquiredLeasesWithWrongToken: AcquiredLease[] = results.map((result) => ({
        resourceKey: (result.acquiredLease as AcquiredLease).resourceKey,
        durationMs: (result.acquiredLease as AcquiredLease).durationMs,
        token: crypto.randomUUID(),
        startTimeUtcMs: (result.acquiredLease as AcquiredLease).startTimeUtcMs,
      }));

      const renewResults = await provider.renew(LEASE_CATEGORY, acquiredLeasesWithWrongToken);

      renewResults.forEach((result, i) => {
        expect(result.statusCode).toBe("InvalidToken");
        expect(result.acquiredLease?.resourceKey).toBe(leaseRequests[i]!.resourceKey);
      });
    },
  );

  orleansTest(
    "DefaultCluster.Tests.GoldenPathInMemoryLeaseProviderTests.Provider_LifeCycle_Acqurie_Renew_Release_ShouldBeAbleToAcquireLeaseOnTheSameResourceAfterRelease",
    async () => {
      const provider = new InMemoryLeaseProvider(new FakeTimeProvider());
      const resourceId = newResourceKey();
      const leaseRequests: LeaseRequest[] = [{ resourceKey: resourceId, durationMs: 15_000 }];

      // Acquire first time.
      const acquireResults1 = await provider.acquire(LEASE_CATEGORY, leaseRequests);

      // Renew.
      const renewResults = await provider.renew(
        LEASE_CATEGORY,
        acquireResults1.map((result) => result.acquiredLease as AcquiredLease),
      );
      renewResults.forEach((result, i) => {
        expect(result.statusCode).toBe("OK");
        expect(result.acquiredLease).not.toBeUndefined();
        expect(result.acquiredLease?.resourceKey).toBe(leaseRequests[i]!.resourceKey);
      });

      // Release.
      await provider.release(
        LEASE_CATEGORY,
        renewResults.map((result) => result.acquiredLease as AcquiredLease),
      );

      // Acquire second time on the same resource after the lease was released.
      const acquireResults2 = await provider.acquire(LEASE_CATEGORY, leaseRequests);
      acquireResults2.forEach((result, i) => {
        expect(result.statusCode).toBe("OK");
        expect(result.acquiredLease).not.toBeUndefined();
        expect(result.acquiredLease?.resourceKey).toBe(leaseRequests[i]!.resourceKey);
        // Token in the two leases should differ.
        expect(result.acquiredLease?.token).not.toBe(acquireResults1[i]!.acquiredLease?.token);
      });
    },
  );
});
