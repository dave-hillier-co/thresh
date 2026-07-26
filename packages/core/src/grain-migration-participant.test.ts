import { describe, expect, it } from "vitest";
import {
  isMigrationParticipant,
  MigrationBag,
  type IGrainMigrationParticipant,
} from "@thresh/core/grain-migration-participant";

describe("isMigrationParticipant", () => {
  it("accepts an object exposing both migration hooks", () => {
    const participant: IGrainMigrationParticipant = {
      onDehydrate: () => undefined,
      onRehydrate: () => undefined,
    };
    expect(isMigrationParticipant(participant)).toBe(true);
  });

  it("rejects objects missing a hook, and non-objects", () => {
    expect(isMigrationParticipant({ onDehydrate: () => undefined })).toBe(false);
    expect(isMigrationParticipant({})).toBe(false);
    expect(isMigrationParticipant(null)).toBe(false);
    expect(isMigrationParticipant(42)).toBe(false);
  });
});

describe("MigrationBag", () => {
  it("round-trips values through set/get/has and exposes its data", () => {
    const bag = new MigrationBag();
    expect(bag.has("count")).toBe(false);
    bag.set("count", 7);
    bag.set("label", "a");
    expect(bag.has("count")).toBe(true);
    expect(bag.get<number>("count")).toBe(7);
    expect(bag.get<string>("label")).toBe("a");
    expect(bag.get("missing")).toBeUndefined();
    expect([...bag.keys].sort()).toEqual(["count", "label"]);
    expect(bag.data).toEqual({ count: 7, label: "a" });
  });

  it("reads from a backing record supplied on the target", () => {
    const bag = new MigrationBag({ count: 5 });
    expect(bag.has("count")).toBe(true);
    expect(bag.get<number>("count")).toBe(5);
  });
});
