import { describe, expect, it } from "vitest";
import {
  backwardCompatible,
  compatibilityDirector,
  strict,
} from "@tsva/core/version-compatibility";

describe("backwardCompatible director", () => {
  it("accepts an implementation newer than or equal to the request", () => {
    expect(backwardCompatible(1, 1)).toBe(true);
    expect(backwardCompatible(1, 2)).toBe(true);
  });

  it("rejects an implementation older than the request", () => {
    expect(backwardCompatible(2, 1)).toBe(false);
  });
});

describe("strict director", () => {
  it("accepts only the exact requested version", () => {
    expect(strict(2, 2)).toBe(true);
    expect(strict(2, 1)).toBe(false);
    expect(strict(1, 2)).toBe(false);
  });
});

describe("compatibilityDirector", () => {
  it("maps kinds to directors, defaulting to backward-compatible", () => {
    expect(compatibilityDirector("backwardCompatible")).toBe(backwardCompatible);
    expect(compatibilityDirector("strict")).toBe(strict);
  });
});
