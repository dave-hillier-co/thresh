import { describe, expect, it } from "vitest";
import { RejectionError } from "@tsva/core/errors";
import { Grain } from "@tsva/core/grain";
import { GrainId } from "@tsva/core/grain-id";
import type {
  DehydrationContext,
  RehydrationContext,
} from "@tsva/core/grain-migration-participant";
import type { InvocationRequest } from "@tsva/core/request";
import { SiloAddress } from "@tsva/core/silo-address";
import { ActivationData } from "@tsva/runtime/activation";
import { FakeTimeProvider } from "@tsva/runtime/test-support/fake-time-provider";

class MigratableGrain extends Grain {
  count = 0;
  onDehydrate(ctx: DehydrationContext): void {
    ctx.set("count", this.count);
  }
  onRehydrate(ctx: RehydrationContext): void {
    const c = ctx.get<number>("count");
    if (c !== undefined) this.count = c;
  }
  async bump(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

const id = new GrainId("Counter", "a");
const silo = new SiloAddress("silo-1", "uid-1", "silo-1:1");

function makeActivation(count: number): { activation: ActivationData; grain: MigratableGrain } {
  const activation = new ActivationData(id, new FakeTimeProvider(), 30_000, false, "act-1");
  const grain = new MigratableGrain();
  grain.setContext(activation);
  grain.count = count;
  activation.instance = grain;
  activation.beginActivate("incoming-call");
  return { activation, grain };
}

const bump = (by: number): InvocationRequest => ({
  target: id,
  interfaceId: 0,
  method: "bump",
  args: [by],
  options: {},
  reentrancyId: "r1",
});

describe("activation migration mechanics", () => {
  it("records a migration request and the directed target", () => {
    const { activation } = makeActivation(0);
    expect(activation.wantsMigration).toBe(false);
    activation.requestMigration(silo);
    expect(activation.wantsMigration).toBe(true);
    expect(activation.migrationTarget?.equals(silo)).toBe(true);
  });

  it("dehydrates in-memory state into the bag via onDehydrate", async () => {
    const { activation } = makeActivation(5);
    const bag = await activation.dehydrate();
    expect(bag).toEqual({ count: 5 });
  });

  it("rejects calls after dehydration as stale, so they re-resolve to the new host", async () => {
    const { activation } = makeActivation(5);
    await activation.dehydrate();
    await expect(activation.invoke(bump(1))).rejects.toMatchObject({
      name: "RejectionError",
      kind: "noActivation",
    });
    await expect(activation.invoke(bump(1))).rejects.toBeInstanceOf(RejectionError);
  });

  it("restores migrated state through applyRehydration", async () => {
    const { activation, grain } = makeActivation(0);
    activation.rehydrationBag = { count: 9 };
    await activation.applyRehydration();
    expect(grain.count).toBe(9);
  });
});
