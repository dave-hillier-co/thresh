import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { UnavailableExceptionFallbackException } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

/**
 * `wireSemanticsForLocalCalls`: a same-silo call round-trips its arguments, result and error
 * through the codec, so it behaves as the same call made from another silo would.
 */
class Money {
  constructor(readonly cents: number) {}
}

class InsufficientFunds extends Error {
  constructor() {
    super("insufficient funds");
    this.name = "InsufficientFunds";
  }
}

const kept: Array<{ items: string[] }> = [];

const Wallet = defineGrain("WireSemanticsWallet", () => {
  const held = { items: ["a"] };
  return {
    keep: async (bag: { items: string[] }): Promise<void> => {
      bag.items.push("touched by callee");
      kept.push(bag);
    },
    held: async (): Promise<{ items: string[] }> => held,
    count: async (): Promise<number> => held.items.length,
    inspect: async (value: unknown): Promise<{ isMoney: boolean; shape: string }> => ({
      isMoney: value instanceof Money,
      shape: JSON.stringify(value),
    }),
    spend: async (): Promise<void> => {
      throw new InsufficientFunds();
    },
  };
});

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.dispose();
  kept.length = 0;
});

async function single(wireSemanticsForLocalCalls?: boolean): Promise<TestSiloHandle> {
  const cluster = await TestCluster.start({
    initialSilos: 1,
    grains: [Wallet],
    reminders: false,
    ...(wireSemanticsForLocalCalls !== undefined ? { wireSemanticsForLocalCalls } : {}),
  });
  clusters.push(cluster);
  return cluster.primary;
}

/** What the call looks like from a second silo, for comparison. */
async function remote(): Promise<{ caller: TestSiloHandle; key: string }> {
  const cluster = await TestCluster.start({
    initialSilos: 2,
    grains: [Wallet],
    reminders: false,
    defaultPlacementStrategy: new PreferLocalPlacement(),
  });
  clusters.push(cluster);
  const [a, b] = cluster.silos as [TestSiloHandle, TestSiloHandle];
  await b.host.getGrain(Wallet, "remote").count();
  expect(b.host.isActive(new GrainId("WireSemanticsWallet", "remote"))).toBe(true);
  return { caller: a, key: "remote" };
}

describe("wireSemanticsForLocalCalls", () => {
  it("does not alias a same-silo call's argument", async () => {
    const silo = await single(true);
    const bag = { items: ["mine"] };
    await silo.host.getGrain(Wallet, "w").keep(bag);
    expect(bag.items).toEqual(["mine"]);
    expect(kept[0]).not.toBe(bag);
  });

  it("does not alias a same-silo call's result", async () => {
    const silo = await single(true);
    const wallet = silo.host.getGrain(Wallet, "w");
    (await wallet.held()).items.push("caller's edit");
    expect(await wallet.count()).toBe(1);
  });

  it("hands a class instance with no surrogate over as the wire does", async () => {
    const silo = await single(true);
    const local = await silo.host.getGrain(Wallet, "w").inspect(new Money(5));
    const { caller, key } = await remote();
    const crossSilo = await caller.host.getGrain(Wallet, key).inspect(new Money(5));
    expect(local).toEqual(crossSilo);
    expect(local.isMoney).toBe(false);
  });

  it("rebuilds an error class the wire cannot rebuild as a remote caller sees it", async () => {
    const silo = await single(true);
    const local = await silo.host
      .getGrain(Wallet, "w")
      .spend()
      .catch((e: unknown) => e);
    const { caller, key } = await remote();
    const crossSilo = await caller.host
      .getGrain(Wallet, key)
      .spend()
      .catch((e: unknown) => e);
    expect(crossSilo).toBeInstanceOf(UnavailableExceptionFallbackException);
    expect(local).toBeInstanceOf(UnavailableExceptionFallbackException);
    expect(local).not.toBeInstanceOf(InsufficientFunds);
    expect((local as Error).message).toBe((crossSilo as Error).message);
  });

  it("is on by default: a same-silo call aliases neither its argument nor its result", async () => {
    const silo = await single();
    const bag = { items: ["mine"] };
    const wallet = silo.host.getGrain(Wallet, "w");
    await wallet.keep(bag);
    expect(bag.items).toEqual(["mine"]);
    expect(kept[0]).not.toBe(bag);
    (await wallet.held()).items.push("caller's edit");
    expect(await wallet.count()).toBe(1);
  });

  it("changes nothing while opted out: a same-silo call shares objects and keeps error classes", async () => {
    const silo = await single(false);
    const bag = { items: ["mine"] };
    const wallet = silo.host.getGrain(Wallet, "w");
    await wallet.keep(bag);
    expect(kept[0]).toBe(bag);
    expect(await wallet.inspect(new Money(5))).toEqual({ isMoney: true, shape: '{"cents":5}' });
    await expect(wallet.spend()).rejects.toBeInstanceOf(InsufficientFunds);
  });
});
