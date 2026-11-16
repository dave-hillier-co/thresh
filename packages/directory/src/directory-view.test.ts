import { describe, expect, it } from "vitest";
import type { MembershipSnapshot, SiloStatus } from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";
import { DirectoryView } from "@thresh/directory/directory-view";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);

function snapshot(version: number, members: Array<[number, SiloStatus]>): MembershipSnapshot {
  return { version, silos: members.map(([n, status]) => ({ address: silo(n), status })) };
}

describe("DirectoryView", () => {
  it("is derived from the membership view alone: version, ring, members", () => {
    const view = DirectoryView.of(
      snapshot(7, [
        [0, "active"],
        [1, "joining"],
        [2, "draining"],
        [3, "active"],
      ]),
    );
    expect(view.version).toBe(7);
    expect(view.ringSilos.map((s) => s.podName).sort()).toEqual(["silo-0", "silo-3"]);
    expect(view.members.map((s) => s.podName).sort()).toEqual([
      "silo-0",
      "silo-1",
      "silo-2",
      "silo-3",
    ]);
    expect(view.isMember(silo(2))).toBe(true);
    expect(view.inRing(silo(2))).toBe(false);
  });

  it("gives every hash one owner, the silo whose ranges hold it, and nothing to a non-ring silo", () => {
    const view = DirectoryView.of(
      snapshot(3, [
        [0, "active"],
        [1, "active"],
        [2, "joining"],
      ]),
    );
    expect(view.rangesOf(silo(2))).toEqual([]);
    for (let i = 0; i < 500; i++) {
      const hash = (i * 0x9e3779b9) >>> 0;
      const owner = view.ownerOf(hash)!;
      const holders = [silo(0), silo(1)].filter((s) =>
        view.rangesOf(s).some((r) => r.contains(hash)),
      );
      expect(holders).toHaveLength(1);
      expect(holders[0]!.equals(owner)).toBe(true);
      expect(view.owns(owner, hash)).toBe(true);
    }
  });

  it("falls back to the joining members while none is active, and owns nothing when empty", () => {
    const cold = DirectoryView.of(
      snapshot(1, [
        [0, "joining"],
        [1, "joining"],
      ]),
    );
    expect(cold.ringSilos).toHaveLength(2);
    expect(DirectoryView.initial.version).toBe(-1);
    expect(DirectoryView.initial.ownerOf(123)).toBeUndefined();
  });

  it("agrees across silos holding the same view, whatever order it lists them in", () => {
    const a = DirectoryView.of(
      snapshot(4, [
        [0, "active"],
        [1, "active"],
      ]),
    );
    const b = DirectoryView.of(
      snapshot(4, [
        [1, "active"],
        [0, "active"],
      ]),
    );
    expect(a.rangesOf(silo(0))).toEqual(b.rangesOf(silo(0)));
  });
});
