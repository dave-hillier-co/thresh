import { describe, expect, it } from "vitest";
import { defineGrain, useTransactionalState } from "@thresh/core/define-grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainType } from "@thresh/core/grain-type";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { SiloAddress } from "@thresh/core/silo-address";
import { TestCluster, type TestSiloHandle } from "@thresh/testing/test-cluster";

interface Balance {
  cents: number;
}

interface Account extends GrainKey<string> {
  deposit(cents: number): Promise<void>;
}

interface Teller extends GrainKey<string> {
  /** Credit `to`, then wait on the test's gate before the transaction resolves. */
  depositThenWait(to: string, cents: number): Promise<void>;
  deposit(to: string, cents: number): Promise<void>;
}

const Account = defineGrainInterface<Account>("RebalanceTxAccount", {
  options: { deposit: { transaction: "join" } },
});
const Teller = defineGrainInterface<Teller>("RebalanceTxTeller", {
  options: {
    depositThenWait: { transaction: "create" },
    deposit: { transaction: "create" },
  },
});

let gate: Promise<void> = Promise.resolve();
let openGate: () => void = () => undefined;

const AccountGrain = defineGrain<Account>(
  "RebalanceTxAccount",
  () => {
    const bal = useTransactionalState<Balance>("balance", { initial: () => ({ cents: 0 }) });
    return {
      deposit: (cents) =>
        bal.performUpdate((s) => {
          s.cents += cents;
        }),
    };
  },
  { placement: "preferLocal" },
);

const TellerGrain = defineGrain<Teller>(
  "RebalanceTxTeller",
  (ctx) => ({
    depositThenWait: async (to, cents) => {
      await ctx.getGrain(Account, to).deposit(cents);
      await gate;
    },
    deposit: async (to, cents) => {
      await ctx.getGrain(Account, to).deposit(cents);
    },
  }),
  { placement: "preferLocal" },
);

const accountId = (key: string) => new GrainId("RebalanceTxAccount" as GrainType, key);

type NodeFacet = { migrateRandomActivations(target: SiloAddress, count: number): Promise<number> };
const nodeOf = (silo: TestSiloHandle): NodeFacet =>
  (silo.host as unknown as { parts: { node: NodeFacet } }).parts.node;

describe("the rebalancer's candidate filter", () => {
  it.each([0, 0.5, 0.999])(
    "skips an activation holding a transaction lock (rng %s)",
    async (rng) => {
      const cluster = await TestCluster.start({
        clusterId: "tx-rebalance-filter",
        initialSilos: 2,
        random: () => rng,
        grains: [
          { ctor: AccountGrain.grain, interfaces: [Account] },
          { ctor: TellerGrain.grain, interfaces: [Teller] },
        ],
      });
      try {
        const [source, target] = [cluster.silos[0]!, cluster.silos[1]!];
        // Idle account: its transaction has resolved, nothing holds it.
        await source.host.getGrain(Teller, "idle-teller").deposit("idle", 1);
        await source.host.getGrain(Teller, "warm-teller").deposit("held", 0);
        // Held account: its transaction is still open.
        gate = new Promise<void>((r) => (openGate = r));
        // Coordinated from the target silo, so no parked turn of its own sits on the source.
        const open = target.host.getGrain(Teller, "held-teller").depositThenWait("held", 1);
        open.catch(() => undefined);
        for (let i = 0; i < 40 && !source.host.isActive(accountId("held")); i++) {
          await new Promise((r) => setTimeout(r, 5));
        }
        await new Promise((r) => setTimeout(r, 20));
        expect(source.host.isActive(accountId("held"))).toBe(true);
        expect(source.host.isActive(accountId("idle"))).toBe(true);

        // Three movable activations remain (two tellers, the idle account). With a
        // single slot per call, each call must spend it on one of them: a blocked
        // activation left in the candidate pool would waste the slot whenever the
        // draw picked it (the migration itself refuses it, returning 0).
        const moves: number[] = [];
        for (let i = 0; i < 4; i++) {
          moves.push(await nodeOf(source).migrateRandomActivations(target.address, 1));
        }

        expect(moves).toEqual([1, 1, 1, 0]);
        expect(source.host.isActive(accountId("held"))).toBe(true);
        expect(source.host.isActive(accountId("idle"))).toBe(false);
        openGate();
        await open;
      } finally {
        openGate();
        await cluster.dispose();
      }
    },
  );
});
