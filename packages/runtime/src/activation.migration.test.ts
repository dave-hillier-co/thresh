import { describe, expect, it } from "vitest";
import { RejectionError } from "@thresh/core/errors";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import type {
  DehydrationContext,
  RehydrationContext,
} from "@thresh/core/grain-migration-participant";
import type { InvocationRequest } from "@thresh/core/request";
import { SiloAddress } from "@thresh/core/silo-address";
import { ActivationData, type ActivationOptions } from "@thresh/runtime/activation";
import { FakeTimeProvider } from "@thresh/runtime/test-support/fake-time-provider";

class MigratableGrain extends Grain {
  count = 0;
  onDehydrate(ctx: DehydrationContext): void {
    ctx.set("count", this.count);
  }
  onRehydrate(ctx: RehydrationContext): void {
    const c = ctx.get<number>("count");
    if (c !== undefined) this.count = c;
  }
  async waitFor(gate: Promise<void>): Promise<void> {
    await gate;
  }
  async bump(by: number): Promise<number> {
    this.count += by;
    return this.count;
  }
}

const id = new GrainId("Counter", "a");
const silo = new SiloAddress("silo-1", "uid-1", "silo-1:1");

function makeActivation(
  count: number,
  options: ActivationOptions = {},
): { activation: ActivationData; grain: MigratableGrain } {
  const activation = new ActivationData(
    id,
    new FakeTimeProvider(),
    30_000,
    false,
    "act-1",
    options,
  );
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

  it("does not run a call that arrives while the migration hand-off is in flight (GH #91)", async () => {
    // The hand-off to the target is the network boundary: hold it open.
    let handOff!: () => void;
    const handedOff = new Promise<void>((r) => (handOff = r));
    const bags: Array<Record<string, unknown>> = [];
    const { activation, grain } = makeActivation(5, {
      deactivationSteps: {
        migrate: async (a) => {
          bags.push(await a.dehydrate());
          await handedOff;
          return true;
        },
      },
    });
    await activation.invoke(bump(0)); // fully activated
    const migrating = activation.deactivate(
      { code: "migrating", description: "test" },
      { migrateTo: silo },
    );
    await new Promise((r) => setTimeout(r, 0));
    expect(bags).toEqual([{ count: 5 }]);

    // Its state has been handed off: a call now must not mutate it here.
    const held = activation.invoke(bump(1));
    let outcome: unknown = "pending";
    void held.then(
      (v) => (outcome = v),
      (e: unknown) => (outcome = e),
    );
    await new Promise((r) => setTimeout(r, 0));
    expect(outcome).toBe("pending"); // held until the migration settles

    handOff();
    await expect(migrating).resolves.toEqual({ migrated: true });
    await expect(held).rejects.toMatchObject({ name: "RejectionError", kind: "noActivation" });
    expect(grain.count).toBe(5);
  });

  it("does not run a call queued ahead of a deactivation's onDeactivate turn (GH #91)", async () => {
    const { activation, grain } = makeActivation(5);
    await activation.invoke(bump(0)); // fully activated
    let release!: () => void;
    const gate = new Promise<void>((r) => (release = r));
    const running = activation.invoke({ ...bump(0), method: "waitFor", args: [gate] });
    await new Promise((r) => setTimeout(r, 0)); // let its turn start
    const queued = activation.invoke({ ...bump(1), reentrancyId: "r2" }); // queued behind it
    const queuedOutcome = expect(queued).rejects.toMatchObject({
      name: "RejectionError",
      kind: "noActivation",
    });
    const deactivating = activation.deactivate({ code: "shutting-down", description: "test" });
    release();
    await running;
    await deactivating;
    await queuedOutcome;
    expect(grain.count).toBe(5);
  });

  it("holds calls once a migration is requested and only rejects them as stale once it settles (GH #91)", async () => {
    const { activation } = makeActivation(5, {
      deactivationSteps: { migrate: async (a) => (await a.dehydrate(), true) },
    });
    await activation.invoke(bump(0));
    activation.requestMigration(silo);

    // A call reaching a migrating activation is HELD, not failed immediately —
    // Orleans reroutes it once the move settles rather than surfacing it as
    // an application error.
    const held = activation.invoke(bump(1));
    let settled = false;
    void held.catch(() => undefined).then(() => (settled = true));
    await Promise.resolve();
    await Promise.resolve();
    expect(settled).toBe(false);

    // The deactivation pipeline that drives the move finishes once the
    // hand-off has settled; only THEN does the held call
    // resolve, signalling the dispatcher to re-resolve to the new host.
    await expect(
      activation.deactivate({ code: "migrating", description: "test" }),
    ).resolves.toEqual({ migrated: true });
    await expect(held).rejects.toMatchObject({
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
