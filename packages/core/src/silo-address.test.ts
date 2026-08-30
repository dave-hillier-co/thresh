import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";

const silo = (podName: string, podUid = "uid", endpoint = "host:1") =>
  new SiloAddress(podName, podUid, endpoint);

// Orleans' `SiloAddress.CompareTo` compares component-by-component with an
// explicit precedence (generation, then port, then address). Thresh's identity
// is podName + podUid + endpoint, and `ringKey` already commits to podName as
// the restart-stable primary key, so that is the precedence here.
describe("SiloAddress.compare", () => {
  it("orders by podName first", () => {
    expect(SiloAddress.compare(silo("a"), silo("b"))).toBeLessThan(0);
    expect(SiloAddress.compare(silo("b"), silo("a"))).toBeGreaterThan(0);
  });

  it("breaks a podName tie on podUid, then on endpoint", () => {
    expect(SiloAddress.compare(silo("a", "u1"), silo("a", "u2"))).toBeLessThan(0);
    expect(SiloAddress.compare(silo("a", "u2"), silo("a", "u1"))).toBeGreaterThan(0);
    expect(SiloAddress.compare(silo("a", "u", "h:1"), silo("a", "u", "h:2"))).toBeLessThan(0);
    expect(SiloAddress.compare(silo("a", "u", "h:2"), silo("a", "u", "h:1"))).toBeGreaterThan(0);
  });

  it("returns 0 exactly when the addresses are equal", () => {
    const a = silo("a", "u", "h:1");
    const b = silo("a", "u", "h:1");
    expect(SiloAddress.compare(a, b)).toBe(0);
    expect(a.equals(b)).toBe(true);
    // The comparator and `equals` must agree, or a sorted candidate list can
    // hold two "distinct" entries that are the same silo.
    expect(SiloAddress.compare(a, silo("a", "u", "h:2")) === 0).toBe(
      a.equals(silo("a", "u", "h:2")),
    );
  });

  it("is ordinal, not locale-sensitive", () => {
    // `localeCompare` puts "a" before "B"; ordinal (UTF-16 code unit) order puts
    // "B" first. Every silo must compute the same order, so the comparison has
    // to be locale-independent.
    expect(SiloAddress.compare(silo("B"), silo("a"))).toBeLessThan(0);
    expect(SiloAddress.compare(silo("silo-10"), silo("silo-9"))).toBeLessThan(0);
  });

  it("is a total order: antisymmetric, transitive and stable under sort", () => {
    const all = [
      silo("b", "u1", "h:1"),
      silo("a", "u2", "h:1"),
      silo("a", "u1", "h:2"),
      silo("a", "u1", "h:1"),
      silo("c", "u1", "h:1"),
    ];
    for (const x of all) {
      for (const y of all) {
        // `+ 0` normalises `-0`, which `toBe` distinguishes from `0`.
        expect(Math.sign(SiloAddress.compare(x, y)) + 0).toBe(
          -Math.sign(SiloAddress.compare(y, x)) + 0,
        );
      }
    }
    const sortedOnce = [...all].sort(SiloAddress.compare).map((s) => s.toString());
    const sortedFromShuffle = [...all]
      .reverse()
      .sort(SiloAddress.compare)
      .map((s) => s.toString());
    expect(sortedFromShuffle).toEqual(sortedOnce);
    expect(sortedOnce).toEqual(["a#u1@h:1", "a#u1@h:2", "a#u2@h:1", "b#u1@h:1", "c#u1@h:1"]);
  });

  it("is usable directly as an Array.prototype.sort comparator", () => {
    // Passed by reference, so it must not close over `this`.
    const compare = SiloAddress.compare;
    expect([silo("b"), silo("a")].sort(compare).map((s) => s.podName)).toEqual(["a", "b"]);
  });

  it("has an instance form matching the static one", () => {
    expect(silo("a").compareTo(silo("b"))).toBeLessThan(0);
    expect(silo("b").compareTo(silo("a"))).toBeGreaterThan(0);
    expect(silo("a").compareTo(silo("a"))).toBe(0);
  });
});
