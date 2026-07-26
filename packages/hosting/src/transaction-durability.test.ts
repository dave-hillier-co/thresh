import { describe, expect, it } from "vitest";
import { grain, transactionalState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainWithStringKey } from "@thresh/core/key-kinds";
import { SiloAddress } from "@thresh/core/silo-address";
import type { TransactionalState } from "@thresh/core/transactional-state";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryTransactionalStorage } from "@thresh/transactions/memory-transactional-storage";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

interface Balance {
  cents: number;
}

interface Account extends GrainWithStringKey {
  credit(cents: number): Promise<void>;
  creditThenFail(cents: number): Promise<void>;
  balance(): Promise<number>;
}

const Account = defineGrainInterface<Account>("TxDurabilityAccount", {
  options: {
    credit: { transaction: "create" },
    creditThenFail: { transaction: "create" },
    balance: { transaction: "createOrJoin" },
  },
});

@grain()
class AccountGrain extends Grain implements Account {
  @transactionalState("balance", { initial: (): Balance => ({ cents: 0 }) })
  private bal!: TransactionalState<Balance>;

  async credit(cents: number): Promise<void> {
    await this.bal.performUpdate((s) => {
      s.cents += cents;
    });
  }

  // Writes inside the transaction, then throws — the boundary must abort, so the
  // staged write never becomes durable.
  async creditThenFail(cents: number): Promise<void> {
    await this.bal.performUpdate((s) => {
      s.cents += cents;
    });
    throw new Error("boom");
  }

  async balance(): Promise<number> {
    return this.bal.performRead((s) => s.cents);
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

// A shared transactional store stands in for the durable backend across a "pod restart".
function buildSilo(storage: MemoryTransactionalStorage): SiloHost {
  return createSilo({ clusterId: "tx-durability", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryTransactionalStorage(storage)
    .registerGrain(AccountGrain, { interfaces: [Account] })
    .build();
}

describe("transactional state durability (Slice 4)", () => {
  it("commits survive deactivation and a silo restart via the durable store", async () => {
    const storage = new MemoryTransactionalStorage();

    const first = buildSilo(storage);
    await first.start();
    await first.getGrain(Account, "acc-1").credit(100);
    await first.getGrain(Account, "acc-1").credit(50);
    await first.stop(); // pod dies

    const restarted = buildSilo(storage); // new pod, same durable store
    await restarted.start();
    try {
      expect(await restarted.getGrain(Account, "acc-1").balance()).toBe(150);
    } finally {
      await restarted.stop();
    }
  });

  it("does not persist an aborted transaction's tentative write", async () => {
    const storage = new MemoryTransactionalStorage();
    const silo = buildSilo(storage);
    await silo.start();
    try {
      await silo.getGrain(Account, "acc-2").credit(40);
      await expect(silo.getGrain(Account, "acc-2").creditThenFail(1000)).rejects.toThrow("boom");
      // The aborted write left nothing behind, even in the live activation.
      expect(await silo.getGrain(Account, "acc-2").balance()).toBe(40);
    } finally {
      await silo.stop();
    }

    const restarted = buildSilo(storage); // same durable store
    await restarted.start();
    try {
      expect(await restarted.getGrain(Account, "acc-2").balance()).toBe(40);
    } finally {
      await restarted.stop();
    }
  });
});
