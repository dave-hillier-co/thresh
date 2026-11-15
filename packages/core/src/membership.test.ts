import { describe, expect, it } from "vitest";
import {
  memberSilos,
  ringSilos,
  type MembershipSnapshot,
  type SiloStatus,
} from "@thresh/core/membership";
import { SiloAddress } from "@thresh/core/silo-address";

const silo = (name: string): SiloAddress => new SiloAddress(name, `uid-${name}`, `${name}:1`);

function view(...members: Array<[string, SiloStatus]>): MembershipSnapshot {
  return {
    version: 1,
    silos: members.map(([name, status]) => ({ address: silo(name), status })),
  };
}

const names = (silos: SiloAddress[]): string[] => silos.map((s) => s.podName).sort();

describe("ringSilos", () => {
  it("is the active members when any member is active", () => {
    const snapshot = view(["a", "active"], ["b", "joining"], ["c", "draining"], ["d", "active"]);
    expect(names(ringSilos(snapshot))).toEqual(["a", "d"]);
  });

  it("is the joining members when no member is active, so a cold cluster can start", () => {
    const snapshot = view(["a", "joining"], ["b", "joining"], ["c", "draining"]);
    expect(names(ringSilos(snapshot))).toEqual(["a", "b"]);
  });

  it("is empty when every member is draining or dead", () => {
    expect(ringSilos(view(["a", "draining"], ["b", "dead"]))).toEqual([]);
  });

  it("never holds a silo that is not a member", () => {
    const snapshot = view(["a", "joining"], ["b", "dead"]);
    const members = new Set(memberSilos(snapshot).map((s) => s.ringKey));
    expect(ringSilos(snapshot).every((s) => members.has(s.ringKey))).toBe(true);
  });
});
