import { describe, expect, it } from "vitest";
import { all, latest, minimum, versionSelector } from "@tsva/core/version-selector";

describe("version selectors", () => {
  it("latest picks the highest compatible version", () => {
    expect(latest(1, [1, 2, 3])).toEqual([3]);
  });

  it("minimum picks the lowest compatible version", () => {
    expect(minimum(1, [1, 2, 3])).toEqual([1]);
  });

  it("all returns every compatible version", () => {
    expect(all(1, [1, 2, 3])).toEqual([1, 2, 3]);
  });

  it("returns empty when nothing is compatible", () => {
    expect(latest(1, [])).toEqual([]);
    expect(minimum(1, [])).toEqual([]);
    expect(all(1, [])).toEqual([]);
  });
});

describe("versionSelector", () => {
  it("maps kinds to selectors, defaulting to latest", () => {
    expect(versionSelector("latest")).toBe(latest);
    expect(versionSelector("minimum")).toBe(minimum);
    expect(versionSelector("all")).toBe(all);
  });
});
