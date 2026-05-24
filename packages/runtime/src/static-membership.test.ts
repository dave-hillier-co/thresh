import { describe, expect, it } from "vitest";
import { activeSilos } from "@tsva/core/membership";
import { SiloAddress } from "@tsva/core/silo-address";
import { StaticMembershipService } from "@tsva/runtime/static-membership";

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:1`);

describe("StaticMembershipService", () => {
  it("starts with the initial silos all active at version 1", () => {
    const m = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    expect(m.current().version).toBe(1);
    expect(activeSilos(m.current())).toHaveLength(2);
    expect(m.localSilo().ringKey).toBe("silo-0");
  });

  it("bumps the version and pushes a snapshot to updates() on change", async () => {
    const m = new StaticMembershipService(silo(0), [silo(0), silo(1)]);
    const iterator = m.updates()[Symbol.asyncIterator]();
    const next = iterator.next();

    m.addSilo(silo(2));

    const { value } = await next;
    expect(value.version).toBe(2);
    expect(activeSilos(value).map((s) => s.ringKey)).toContain("silo-2");
  });

  it("removes a silo from the view", () => {
    const m = new StaticMembershipService(silo(0), [silo(0), silo(1), silo(2)]);
    m.removeSilo(silo(1));
    expect(activeSilos(m.current()).map((s) => s.ringKey)).toEqual(["silo-0", "silo-2"]);
    expect(m.current().version).toBe(2);
  });

  it("does not re-add a silo already in the view", () => {
    const m = new StaticMembershipService(silo(0), [silo(0)]);
    m.addSilo(silo(0));
    expect(m.current().version).toBe(1);
  });
});
