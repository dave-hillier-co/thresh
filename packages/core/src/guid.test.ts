import { describe, expect, it } from "vitest";
import { Guid } from "@thresh/core/guid";

describe("Guid", () => {
  it("round-trips through its string form", () => {
    const s = "550e8400-e29b-41d4-a716-446655440000";
    expect(Guid.parse(s).toString()).toBe(s);
  });

  it("rejects a non-uuid string", () => {
    expect(() => Guid.parse("not-a-guid")).toThrow();
  });

  it("generates distinct random guids", () => {
    expect(Guid.newGuid().equals(Guid.newGuid())).toBe(false);
  });

  it("compares by value", () => {
    const s = "550e8400-e29b-41d4-a716-446655440000";
    expect(Guid.parse(s).equals(Guid.parse(s))).toBe(true);
  });
});
