import { describe, expect, it } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { TestCluster } from "@thresh/testing/test-cluster";

interface Balance {
  cents: number;
}

interface Account extends GrainWithStringKey {
  deposit(cents: number): Promise<void>;
  withdraw(cents: number): Promise<void>;
  balance(): Promise<number>;
}

interface Teller extends GrainWithStringKey {
  fund(account: string, cents: number): Promise<void>;
  transfer(from: string, to: string, cents: number): Promise<void>;
}

const Account = defineGrainInterface<Account>("ClusterTxAccount", {
  options: {
    deposit: { transaction: "join" },
    withdraw: { transaction: "join" },
    balance: { transaction: "createOrJoin" },
  },
});
const Teller = defineGrainInterface<Teller>("ClusterTxTeller", {
  options: { fund: { transaction: "create" }, transfer: { transaction: "create" } },
});

// preferLocal so a grain's first caller's silo hosts it — lets the test place
// accounts on specific silos by where it first touches them.
const AccountGrain = defineGrain<Account>(
  "ClusterTxAccount",
  () => {
    const bal = useTransactionalState<Balance>("balance", { initial: () => ({ cents: 0 }) });
    return {
      deposit: (cents) =>
        bal.performUpdate((s) => {
          s.cents += cents;
        }),
      withdraw: (cents) =>
        bal.performUpdate((s) => {
          if (s.cents < cents) throw new Error("insufficient funds");
          s.cents -= cents;
        }),
      balance: () => bal.performRead((s) => s.cents),
    };
  },
  { placement: "preferLocal" },
);

const TellerGrain = defineGrain<Teller>(
  "ClusterTxTeller",
  (ctx) => ({
    fund: async (account, cents) => {
      await ctx.getGrain(Account, account).deposit(cents);
    },
    transfer: async (from, to, cents) => {
      await ctx.getGrain(Account, to).deposit(cents);
      await ctx.getGrain(Account, from).withdraw(cents);
    },
  }),
  { placement: "preferLocal" },
);

const accountId = (key: string) => new GrainId("ClusterTxAccount" as GrainType, key);

function buildCluster() {
  return TestCluster.start({
    clusterId: "tx-cluster",
    grains: [
      { ctor: AccountGrain.grain, interfaces: [Account] },
      { ctor: TellerGrain.grain, interfaces: [Teller] },
    ],
  });
}

describe("cross-silo transactions (Slice 4c)", () => {
  it("commits a transfer spanning grains on two different silos", async () => {
    const cluster = await buildCluster();
    try {
      // Place A on silo-0 and B on silo-1 via preferLocal first-touch.
      await cluster.silos[0]!.host.getGrain(Teller, "t0").fund("A", 100);
      await cluster.silos[1]!.host.getGrain(Teller, "t1").fund("B", 0);
      expect(cluster.silos[0]!.host.isActive(accountId("A"))).toBe(true);
      expect(cluster.silos[1]!.host.isActive(accountId("B"))).toBe(true);

      // Transfer initiated on silo-0 spans A (local) and B (remote on silo-1).
      await cluster.silos[0]!.host.getGrain(Teller, "t0").transfer("A", "B", 30);

      expect(await cluster.silos[0]!.host.getGrain(Account, "A").balance()).toBe(70);
      expect(await cluster.silos[0]!.host.getGrain(Account, "B").balance()).toBe(30);
    } finally {
      await cluster.dispose();
    }
  });

  it("aborts a cross-silo transfer on overdraft, releasing the remote participant", async () => {
    const cluster = await buildCluster();
    try {
      await cluster.silos[0]!.host.getGrain(Teller, "t0").fund("A", 50);
      await cluster.silos[1]!.host.getGrain(Teller, "t1").fund("B", 0);

      await expect(
        cluster.silos[0]!.host.getGrain(Teller, "t0").transfer("A", "B", 100),
      ).rejects.toThrow(/insufficient/);

      // Both sides rolled back; the remote credit to B was undone and its lock released
      // (a subsequent transfer succeeds, which it could not if B stayed locked).
      expect(await cluster.silos[0]!.host.getGrain(Account, "A").balance()).toBe(50);
      expect(await cluster.silos[0]!.host.getGrain(Account, "B").balance()).toBe(0);
      await cluster.silos[0]!.host.getGrain(Teller, "t0").transfer("A", "B", 20);
      expect(await cluster.silos[0]!.host.getGrain(Account, "A").balance()).toBe(30);
      expect(await cluster.silos[0]!.host.getGrain(Account, "B").balance()).toBe(20);
    } finally {
      await cluster.dispose();
    }
  });
});
