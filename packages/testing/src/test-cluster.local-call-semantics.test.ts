import { afterEach, describe, expect, it } from "vitest";
import { defineGrain } from "@thresh/core/define-grain";
import { UnavailableExceptionFallbackException } from "@thresh/core/errors";
import { GrainId } from "@thresh/core/grain-id";
import { immutable } from "@thresh/core/immutable";
import { UnsupportedValueError } from "@thresh/core/value-codec";
import { PreferLocalPlacement } from "@thresh/runtime/placement/prefer-local-placement";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";
import type { LocalCallSemantics } from "@thresh/runtime/local-call-semantics-dispatcher";

/**
 * `localCallSemantics`: what a same-silo call does to its arguments, result and error. "copy" and
 * "wire" both make it behave as the same call made from another silo would; "reference" shares
 * the caller's objects.
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
const sharedHeld = immutable({ items: ["shared"] });

const Wallet = defineGrain("LocalCallSemanticsWallet", () => {
  const held = { items: ["a"] };
  return {
    keep: async (bag: { items: string[] }): Promise<void> => {
      kept.push(bag);
    },
    keepAndTouch: async (bag: { items: string[] }): Promise<void> => {
      bag.items.push("touched by callee");
      kept.push(bag);
    },
    held: async (): Promise<{ items: string[] }> => held,
    sharedHeld: async (): Promise<{ items: string[] }> => sharedHeld,
    count: async (): Promise<number> => held.items.length,
    inspect: async (value: unknown): Promise<{ isMoney: boolean; shape: string }> => ({
      isMoney: value instanceof Money,
      shape: JSON.stringify(value),
    }),
    echo: async (value: unknown): Promise<unknown> => value,
    spend: async (): Promise<void> => {
      throw new InsufficientFunds();
    },
    leak: async (): Promise<unknown> => () => 1,
  };
});

const clusters: TestCluster[] = [];
afterEach(async () => {
  for (const c of clusters.splice(0)) await c.dispose();
  kept.length = 0;
});

async function single(localCallSemantics?: LocalCallSemantics): Promise<TestSiloHandle> {
  const cluster = await TestCluster.start({
    initialSilos: 1,
    grains: [Wallet],
    reminders: false,
    ...(localCallSemantics !== undefined ? { localCallSemantics } : {}),
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
  expect(b.host.isActive(new GrainId("LocalCallSemanticsWallet", "remote"))).toBe(true);
  return { caller: a, key: "remote" };
}

describe.each<LocalCallSemantics>(["copy", "wire"])(
  'localCallSemantics: "%s" gives a same-silo call cross-silo semantics',
  (semantics) => {
    it("does not alias a same-silo call's argument", async () => {
      const silo = await single(semantics);
      const bag = { items: ["mine"] };
      await silo.host.getGrain(Wallet, "w").keepAndTouch(bag);
      expect(bag.items).toEqual(["mine"]);
      expect(kept[0]).not.toBe(bag);
    });

    it("does not alias a same-silo call's result", async () => {
      const silo = await single(semantics);
      const wallet = silo.host.getGrain(Wallet, "w");
      (await wallet.held()).items.push("caller's edit");
      expect(await wallet.count()).toBe(1);
    });

    it("hands a class instance with no surrogate over as the wire does", async () => {
      const silo = await single(semantics);
      const local = await silo.host.getGrain(Wallet, "w").inspect(new Money(5));
      const { caller, key } = await remote();
      const crossSilo = await caller.host.getGrain(Wallet, key).inspect(new Money(5));
      expect(local).toEqual(crossSilo);
      expect(local.isMoney).toBe(false);
    });

    it("returns values as the wire does", async () => {
      const silo = await single(semantics);
      const value = [new Map([["k", new Set([1n])]]), new Date(3), undefined, { gone: undefined }];
      const local = await silo.host.getGrain(Wallet, "w").echo(value);
      const { caller, key } = await remote();
      const crossSilo = await caller.host.getGrain(Wallet, key).echo(value);
      expect(local).toEqual(crossSilo);
      expect(local).toStrictEqual([new Map([["k", new Set([1n])]]), new Date(3), undefined, {}]);
    });

    it("refuses an argument the wire cannot carry", async () => {
      const silo = await single(semantics);
      await expect(silo.host.getGrain(Wallet, "w").echo({ fn: () => 1 })).rejects.toBeInstanceOf(
        UnsupportedValueError,
      );
    });

    it("fails a call whose result the wire cannot carry, as a remote caller sees it", async () => {
      const silo = await single(semantics);
      const local = await silo.host
        .getGrain(Wallet, "w")
        .leak()
        .catch((e: unknown) => e);
      const { caller, key } = await remote();
      const crossSilo = await caller.host
        .getGrain(Wallet, key)
        .leak()
        .catch((e: unknown) => e);
      expect((local as Error).constructor).toBe((crossSilo as Error).constructor);
      expect((local as Error).message).toBe((crossSilo as Error).message);
    });

    it("rebuilds an error class the wire cannot rebuild as a remote caller sees it", async () => {
      const silo = await single(semantics);
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
  },
);

describe('localCallSemantics: "copy" and an immutable value', () => {
  it("shares a marked argument with the callee", async () => {
    const silo = await single("copy");
    const bag = immutable({ items: ["mine"] });
    await silo.host.getGrain(Wallet, "w").keep(bag);
    expect(kept[0]).toBe(bag);
  });

  it("shares a marked result with the caller", async () => {
    const silo = await single("copy");
    expect(await silo.host.getGrain(Wallet, "w").sharedHeld()).toBe(sharedHeld);
  });

  it('copies a marked value under "wire", which ignores the mark', async () => {
    const silo = await single("wire");
    const bag = immutable({ items: ["mine"] });
    await silo.host.getGrain(Wallet, "w").keep(bag);
    expect(kept[0]).not.toBe(bag);
    expect(kept[0]).toEqual(bag);
  });
});

describe("localCallSemantics defaults and opt-out", () => {
  it('is "wire" by default in TestCluster: no aliasing, and a marked value is still copied', async () => {
    const silo = await single();
    const bag = { items: ["mine"] };
    const wallet = silo.host.getGrain(Wallet, "w");
    await wallet.keepAndTouch(bag);
    expect(bag.items).toEqual(["mine"]);
    expect(kept[0]).not.toBe(bag);
    (await wallet.held()).items.push("caller's edit");
    expect(await wallet.count()).toBe(1);
    expect(await wallet.sharedHeld()).not.toBe(sharedHeld);
  });

  it('shares objects and keeps error classes under "reference"', async () => {
    const silo = await single("reference");
    const bag = { items: ["mine"] };
    const wallet = silo.host.getGrain(Wallet, "w");
    await wallet.keep(bag);
    expect(kept[0]).toBe(bag);
    expect(await wallet.inspect(new Money(5))).toEqual({ isMoney: true, shape: '{"cents":5}' });
    await expect(wallet.spend()).rejects.toBeInstanceOf(InsufficientFunds);
  });
});
