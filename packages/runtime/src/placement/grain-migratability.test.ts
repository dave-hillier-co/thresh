import { describe, expect, it } from "vitest";
import type { GrainMetadata } from "@thresh/core/grain-metadata";
import { isMigratable } from "@thresh/runtime/placement/grain-migratability";

const metadata = (options: GrainMetadata["options"] = {}): GrainMetadata =>
  ({ grainType: "Counter", options }) as GrainMetadata;

describe("isMigratable (Orleans GrainMigratabilityChecker)", () => {
  it("moves an ordinary registered grain", () => {
    expect(isMigratable("Counter", "rebalancer", metadata())).toBe(true);
    expect(isMigratable("Counter", "repartitioner", metadata())).toBe(true);
  });

  it("never moves a stateless worker", () => {
    expect(isMigratable("Worker", "rebalancer", metadata({ stateless: true }))).toBe(false);
    expect(isMigratable("Worker", "repartitioner", metadata({ stateless: true }))).toBe(false);
  });

  it("never moves a system or client grain", () => {
    expect(isMigratable("$management", "rebalancer", metadata())).toBe(false);
    expect(isMigratable("$system.broadcastPublisher", "repartitioner", metadata())).toBe(false);
    expect(isMigratable("$client", "rebalancer", metadata())).toBe(false);
  });

  it("honours the immovable marker per mover", () => {
    expect(isMigratable("Counter", "rebalancer", metadata({ immovable: "any" }))).toBe(false);
    expect(isMigratable("Counter", "rebalancer", metadata({ immovable: "rebalancer" }))).toBe(
      false,
    );
    expect(isMigratable("Counter", "repartitioner", metadata({ immovable: "rebalancer" }))).toBe(
      true,
    );
  });

  it("treats a type with no registration as immovable", () => {
    expect(isMigratable("Unknown", "rebalancer", undefined)).toBe(false);
  });
});
