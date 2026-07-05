import { describe, expect, it } from "vitest";
import { InMemoryLeaseProvider } from "@tsva/core/lease-provider";
import { FakeTimeProvider } from "@tsva/core/test-support/fake-time-provider";

describe("InMemoryLeaseProvider", () => {
  it("lets another caller acquire once the lease has expired", async () => {
    const time = new FakeTimeProvider();
    const provider = new InMemoryLeaseProvider(time);
    const resourceKey = "res";

    const first = await provider.acquire("cat", [{ resourceKey, durationMs: 1_000 }]);
    expect(first[0]?.statusCode).toBe("OK");

    // Still held: a second caller is refused.
    const stillHeld = await provider.acquire("cat", [{ resourceKey, durationMs: 1_000 }]);
    expect(stillHeld[0]?.statusCode).toBe("LeaseNotAvailable");

    // Past expiry: a new caller can now acquire it, with a fresh token.
    time.advance(1_001);
    const afterExpiry = await provider.acquire("cat", [{ resourceKey, durationMs: 1_000 }]);
    expect(afterExpiry[0]?.statusCode).toBe("OK");
    expect(afterExpiry[0]?.acquiredLease?.token).not.toBe(first[0]?.acquiredLease?.token);
  });

  it("lets the owner renew an already-expired lease by token, extending it", async () => {
    const time = new FakeTimeProvider();
    const provider = new InMemoryLeaseProvider(time);
    const resourceKey = "res";

    const acquired = (await provider.acquire("cat", [{ resourceKey, durationMs: 1_000 }]))[0]!
      .acquiredLease!;

    time.advance(1_001);
    const renewed = await provider.renew("cat", [acquired]);
    expect(renewed[0]?.statusCode).toBe("OK");
    expect(renewed[0]?.acquiredLease?.token).not.toBe(acquired.token);

    // The renewal is in effect: a fresh acquire attempt is refused.
    const blocked = await provider.acquire("cat", [{ resourceKey, durationMs: 1_000 }]);
    expect(blocked[0]?.statusCode).toBe("LeaseNotAvailable");
  });

  it("scopes leases by category, not just resource key", async () => {
    const provider = new InMemoryLeaseProvider(new FakeTimeProvider());
    const resourceKey = "shared-key";

    const inCatA = await provider.acquire("catA", [{ resourceKey, durationMs: 1_000 }]);
    const inCatB = await provider.acquire("catB", [{ resourceKey, durationMs: 1_000 }]);

    expect(inCatA[0]?.statusCode).toBe("OK");
    expect(inCatB[0]?.statusCode).toBe("OK");
  });
});
