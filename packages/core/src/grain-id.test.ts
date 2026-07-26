import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { Guid } from "@thresh/core/guid";

describe("GrainId", () => {
  it("renders a string key as Type/key", () => {
    expect(new GrainId("Counter", "tenant-42").toString()).toBe("Counter/tenant-42");
  });

  it("tracks the key kind", () => {
    expect(new GrainId("Counter", "x").keyKind).toBe("string");
    expect(new GrainId("Counter", 42n).keyKind).toBe("integer");
    expect(new GrainId("Device", Guid.parse("550e8400-e29b-41d4-a716-446655440000")).keyKind).toBe(
      "guid",
    );
  });

  it("round-trips each key kind through parse", () => {
    const guid = Guid.newGuid();
    for (const id of [
      new GrainId("Counter", "tenant-42"),
      new GrainId("Counter", 42n),
      new GrainId("Device", guid),
    ]) {
      const reparsed = GrainId.parse(id.toString(), id.keyKind);
      expect(reparsed.equals(id)).toBe(true);
    }
  });

  it("splits only on the first slash so keys may contain slashes", () => {
    const id = GrainId.parse("Counter/a/b/c", "string");
    expect(id.type).toBe("Counter");
    expect(id.key).toBe("a/b/c");
  });

  it("has a stable, deterministic uniform hash", () => {
    const a = new GrainId("Counter", "tenant-42").getUniformHashCode();
    const b = new GrainId("Counter", "tenant-42").getUniformHashCode();
    expect(a).toBe(b);
    expect(Number.isInteger(a)).toBe(true);
    expect(a).toBeGreaterThanOrEqual(0);
    expect(a).toBeLessThanOrEqual(0xffffffff);
  });

  it("hashes different identities to different codes", () => {
    expect(new GrainId("Counter", "a").getUniformHashCode()).not.toBe(
      new GrainId("Counter", "b").getUniformHashCode(),
    );
  });

  it("compares by value", () => {
    expect(new GrainId("Counter", "x").equals(new GrainId("Counter", "x"))).toBe(true);
    expect(new GrainId("Counter", "x").equals(new GrainId("Counter", "y"))).toBe(false);
    expect(new GrainId("Counter", "x").equals(new GrainId("Other", "x"))).toBe(false);
  });
});
