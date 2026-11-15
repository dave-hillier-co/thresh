import { describe, expect, it } from "vitest";
import { GrainId } from "@thresh/core/grain-id";
import { Guid } from "@thresh/core/guid";
import { decodeValue, encodeValue } from "@thresh/core/value-codec";

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

  describe("one identity: type and canonical key text", () => {
    it("treats a string key and an integer key with the same text as the same grain", () => {
      const asString = new GrainId("Counter", "42");
      const asInteger = new GrainId("Counter", 42n);
      expect(asString.equals(asInteger)).toBe(true);
      expect(asInteger.equals(asString)).toBe(true);
      expect(asString.toString()).toBe(asInteger.toString());
      expect(asString.getUniformHashCode()).toBe(asInteger.getUniformHashCode());
    });

    it("treats a string key and a guid key with the same text as the same grain", () => {
      const guid = Guid.newGuid();
      expect(new GrainId("Device", guid.toString()).equals(new GrainId("Device", guid))).toBe(true);
    });

    it("reads the key text as the kind the grain type declares", () => {
      const read = new GrainId("Counter", "42").asKind("integer");
      expect(read.key).toBe(42n);
      expect(read.keyKind).toBe("integer");
      expect(read.equals(new GrainId("Counter", "42"))).toBe(true);
      const guid = Guid.newGuid();
      expect(new GrainId("Device", guid.toString()).asKind("guid").key).toBeInstanceOf(Guid);
    });

    it("names the id when the text cannot be read as the declared kind", () => {
      expect(() => new GrainId("Counter", "not-a-number").asKind("integer")).toThrow(
        /Counter\/not-a-number/,
      );
    });

    it("keeps the identity across the codec whichever kind built it", () => {
      for (const id of [new GrainId("Counter", "42"), new GrainId("Counter", 42n)]) {
        const decoded = decodeValue(encodeValue(id)) as GrainId;
        expect(decoded.equals(id)).toBe(true);
        expect(decoded.toString()).toBe("Counter/42");
      }
    });

    it("resolves an id persisted with a key kind and one persisted without", () => {
      const encoded = encodeValue(new GrainId("Counter", 42n)) as Record<string, unknown>;
      const withKind = decodeValue({ ...encoded, keyKind: "integer" }) as GrainId;
      const withoutKind = decodeValue({ ...encoded, keyKind: undefined }) as GrainId;
      expect(withKind.equals(new GrainId("Counter", "42"))).toBe(true);
      expect(withoutKind.equals(new GrainId("Counter", 42n))).toBe(true);
    });
  });
});
