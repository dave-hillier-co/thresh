import { describe, expect, it } from "vitest";
import { newActivationId } from "@thresh/core/activation-id";
import { RejectionError } from "@thresh/core/errors";
import type { GrainAddress } from "@thresh/core/grain-address";
import { GrainId } from "@thresh/core/grain-id";
import { SiloAddress } from "@thresh/core/silo-address";
import { LocalDirectoryPartition } from "@thresh/directory/local-directory-partition";
import { LocationCache } from "@thresh/directory/location-cache";

const siloA = new SiloAddress("silo-A", "ua", "a:1");
const siloB = new SiloAddress("silo-B", "ub", "b:1");
const addr = (key: string, silo: SiloAddress, activationId = newActivationId()): GrainAddress => ({
  grainId: new GrainId("Counter", key),
  silo,
  activationId,
});

describe("LocalDirectoryPartition", () => {
  it("registers a new grain and looks it up", () => {
    const dir = new LocalDirectoryPartition();
    const a = addr("x", siloA);
    expect(dir.register(a)).toBe(a);
    expect(dir.lookup(a.grainId)).toEqual(a);
  });

  it("is compare-and-set: a racing second registration returns the first winner", () => {
    const dir = new LocalDirectoryPartition();
    const first = addr("x", siloA);
    const second = addr("x", siloB);
    expect(dir.register(first)).toBe(first);
    // Loser observes the winner and must forward to it.
    expect(dir.register(second)).toBe(first);
    expect(dir.lookup(first.grainId)).toEqual(first);
  });

  it("replaces an entry only when the expected previous is supplied", () => {
    const dir = new LocalDirectoryPartition();
    const stale = addr("x", siloA);
    const moved = addr("x", siloB);
    dir.register(stale);
    expect(dir.register(moved, stale)).toBe(moved);
    expect(dir.lookup(moved.grainId)).toEqual(moved);
  });

  it("ignores a replace whose expected previous does not match", () => {
    const dir = new LocalDirectoryPartition();
    const current = addr("x", siloA);
    const wrongPrevious = addr("x", siloB);
    const attempt = addr("x", siloB);
    dir.register(current);
    expect(dir.register(attempt, wrongPrevious)).toBe(current);
  });

  it("treats an entry owned by a dead silo as overwritable, without needing the exact previous (Orleans RegisterCore)", () => {
    const dir = new LocalDirectoryPartition((silo) => !silo.equals(siloA));
    const dead = addr("x", siloA);
    const fresh = addr("x", siloB);
    dir.register(dead);
    expect(dir.register(fresh)).toBe(fresh);
    expect(dir.lookup(fresh.grainId)).toEqual(fresh);
  });

  it("still applies the CAS rule when the existing entry's silo is live", () => {
    const dir = new LocalDirectoryPartition(() => true);
    const first = addr("x", siloA);
    const second = addr("x", siloB);
    dir.register(first);
    expect(dir.register(second)).toBe(first);
  });

  it("rejects a brand-new registration from a caller behind an in-flight recovery, so it retries against a fresher view", () => {
    const dir = new LocalDirectoryPartition();
    dir.beginRecovery(5);
    const fresh = addr("x", siloA);
    expect(() => dir.register(fresh, undefined, 4)).toThrow(RejectionError);
    expect(dir.lookup(fresh.grainId)).toBeUndefined();
  });

  it("allows a brand-new registration once the caller's version catches up to the recovery", () => {
    const dir = new LocalDirectoryPartition();
    dir.beginRecovery(5);
    const fresh = addr("x", siloA);
    expect(dir.register(fresh, undefined, 5)).toBe(fresh);
  });

  it("allows recovery itself (no caller version) to register during its own in-flight window", () => {
    const dir = new LocalDirectoryPartition();
    dir.beginRecovery(5);
    const recovered = addr("x", siloA);
    expect(dir.register(recovered)).toBe(recovered);
  });

  it("clears the recovery gate once ended, and a stale endRecovery for a superseded version is a no-op", () => {
    const dir = new LocalDirectoryPartition();
    dir.beginRecovery(5);
    dir.endRecovery(4); // stale call for an earlier recovery — must not clear version 5's gate
    expect(dir.recoveryMembershipVersion).toBe(5);
    dir.endRecovery(5);
    expect(dir.recoveryMembershipVersion).toBeUndefined();
    const fresh = addr("x", siloA);
    expect(dir.register(fresh, undefined, 0)).toBe(fresh);
  });

  it("unregisters only the matching activation", () => {
    const dir = new LocalDirectoryPartition();
    const a = addr("x", siloA);
    dir.register(a);
    dir.unregister({ ...a, activationId: "different" });
    expect(dir.lookup(a.grainId)).toEqual(a);
    dir.unregister(a);
    expect(dir.lookup(a.grainId)).toBeUndefined();
  });

  it("bulk-unregisters every entry pointing at a departed silo", () => {
    const dir = new LocalDirectoryPartition();
    dir.register(addr("x", siloA));
    dir.register(addr("y", siloB));
    dir.register(addr("z", siloA));
    dir.unregisterSilo(siloA);
    expect(dir.size).toBe(1);
    expect(dir.lookup(new GrainId("Counter", "y"))).toBeDefined();
  });

  it("drain classifies every entry in one pass, removing drops and handoffs", () => {
    const dir = new LocalDirectoryPartition();
    const x = addr("x", siloA);
    const y = addr("y", siloB);
    const z = addr("z", siloA);
    dir.register(x);
    dir.register(y);
    dir.register(z);

    // Drop the siloB host, hand off x, keep z.
    const handed = dir.drain((e) =>
      e.silo.equals(siloB) ? "drop" : e.grainId.equals(x.grainId) ? "handoff" : "keep",
    );

    expect(handed).toEqual([x]);
    expect(dir.size).toBe(1);
    expect(dir.lookup(z.grainId)).toEqual(z);
    expect(dir.lookup(x.grainId)).toBeUndefined();
    expect(dir.lookup(y.grainId)).toBeUndefined();
  });

  it("acceptHandoff registers entries it still owns", () => {
    const dir = new LocalDirectoryPartition();
    const a = addr("x", siloA);
    dir.acceptHandoff([a], () => true);
    expect(dir.lookup(a.grainId)).toEqual(a);
  });

  it("acceptHandoff never overwrites a fresher entry (a concurrent reactivation wins)", () => {
    const dir = new LocalDirectoryPartition();
    const fresh = addr("x", siloB);
    const stale = addr("x", siloA, "stale-activation");
    dir.register(fresh);
    dir.acceptHandoff([stale], () => true);
    expect(dir.lookup(fresh.grainId)).toEqual(fresh);
  });

  it("acceptHandoff ignores entries whose range has moved away (the versioned guard)", () => {
    const dir = new LocalDirectoryPartition();
    const a = addr("x", siloA);
    dir.acceptHandoff([a], () => false); // no longer owned here
    expect(dir.lookup(a.grainId)).toBeUndefined();
    expect(dir.size).toBe(0);
  });

  it("treats a lookup whose host silo is no longer live as a miss (Orleans IsSiloDead)", () => {
    // Sociable test: register a grain owned by siloA, then shift the membership
    // view so siloA is no longer live and assert the partition returns undefined
    // rather than the stale pointer.
    const live = new Set<string>([siloA.ringKey, siloB.ringKey]);
    const dir = new LocalDirectoryPartition((silo) => live.has(silo.ringKey));
    const a = addr("x", siloA);
    dir.register(a);
    expect(dir.lookup(a.grainId)).toEqual(a);

    live.delete(siloA.ringKey); // siloA falls out of membership

    expect(dir.lookup(a.grainId)).toBeUndefined();
  });

  it("entriesIn returns the live entries matching a predicate", () => {
    const dir = new LocalDirectoryPartition();
    const x = addr("x", siloA);
    const y = addr("y", siloB);
    dir.register(x);
    dir.register(y);
    expect(dir.entriesIn((e) => e.silo.equals(siloA))).toEqual([x]);
  });
});

describe("LocationCache", () => {
  it("serves a cached hit without a directory round-trip, and re-resolves after invalidation", () => {
    const cache = new LocationCache();
    const dir = new LocalDirectoryPartition();
    const a = addr("x", siloA);
    dir.register(a);

    let lookups = 0;
    const resolve = (grainId: GrainId): GrainAddress | undefined => {
      const hit = cache.get(grainId);
      if (hit !== undefined) return hit;
      lookups++;
      const found = dir.lookup(grainId);
      if (found !== undefined) cache.put(found);
      return found;
    };

    expect(resolve(a.grainId)).toEqual(a);
    expect(resolve(a.grainId)).toEqual(a);
    expect(lookups).toBe(1); // second call served from cache

    cache.invalidate(a.grainId); // e.g. after a rejected send
    expect(resolve(a.grainId)).toEqual(a);
    expect(lookups).toBe(2); // re-resolved through the directory
  });

  it("drops cached entries pointing at a departed silo", () => {
    const cache = new LocationCache();
    cache.put(addr("x", siloA));
    cache.put(addr("y", siloB));
    cache.invalidateSilo(siloA);
    expect(cache.get(new GrainId("Counter", "x"))).toBeUndefined();
    expect(cache.get(new GrainId("Counter", "y"))).toBeDefined();
  });
});
