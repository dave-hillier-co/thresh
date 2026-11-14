import { describe, expect, it } from "vitest";
import { Grain } from "./grain";
import { isObserverMethod, isRemotelyCallable } from "./remote-method-surface";

class UserBase extends Grain {
  fromBase(): void {}
}
class Leaf extends UserBase {
  readonly arrow = (): void => {};
  own(): void {}
}

describe("remote method surface", () => {
  it("includes own, user-base and class-field methods", () => {
    const leaf = new Leaf();
    for (const name of ["own", "fromBase", "arrow"]) {
      expect(isRemotelyCallable(leaf, name)).toBe(true);
    }
  });

  it("excludes constructor, runtime hooks, framework members and Object.prototype", () => {
    const leaf = new Leaf();
    for (const name of [
      "constructor",
      "onActivate",
      "onDeactivate",
      "setContext",
      "onDehydrate",
      "onRehydrate",
      "toString",
      "hasOwnProperty",
      "missing",
    ]) {
      expect(isRemotelyCallable(leaf, name)).toBe(false);
    }
  });

  it("limits an observer to its own and user-prototype methods", () => {
    class Obs {
      ping(): void {}
    }
    expect(isObserverMethod(new Obs(), "ping")).toBe(true);
    expect(isObserverMethod({ x: () => 1 }, "x")).toBe(true);
    for (const name of ["hasOwnProperty", "toString", "constructor"]) {
      expect(isObserverMethod(new Obs(), name)).toBe(false);
    }
  });
});
