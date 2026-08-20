import { describe, expect, it } from "vitest";
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { createSilo } from "@thresh/hosting/silo-builder";

interface BalanceState {
  cents: number;
}

interface IAccount extends GrainKey<string> {
  deposit(cents: number): Promise<number>;
  getBalance(): Promise<number>;
}
const IAccount = defineGrainInterface<IAccount>("IAccount.persist", {
  options: { getBalance: { readOnly: true } },
});

@grain()
class AccountGrain extends Grain implements IAccount {
  @persistentState("balance", { defaultValue: (): BalanceState => ({ cents: 0 }) })
  private balance!: PersistentState<BalanceState>;

  async deposit(cents: number): Promise<number> {
    this.balance.value.cents += cents;
    await this.balance.write();
    return this.balance.value.cents;
  }

  async getBalance(): Promise<number> {
    return this.balance.value.cents; // served from memory, populated by read-on-activate
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

// A shared store stands in for the durable backend across "pod restarts".
function buildSilo(storage: MemoryGrainStorage) {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryStorage(storage)
    .registerGrain(AccountGrain, { interfaces: [IAccount] })
    .build();
}

describe("persistence end-to-end", () => {
  it("survives deactivation and a silo restart via the durable store", async () => {
    const storage = new MemoryGrainStorage();

    const first = buildSilo(storage);
    await first.start();
    expect(await first.getGrain(IAccount, "acc-1").deposit(100)).toBe(100);
    expect(await first.getGrain(IAccount, "acc-1").deposit(50)).toBe(150);
    await first.stop(); // pod dies

    const restarted = buildSilo(storage); // new pod, same durable store
    await restarted.start();
    try {
      // Read-on-activate repopulates the grain's state.
      expect(await restarted.getGrain(IAccount, "acc-1").getBalance()).toBe(150);
    } finally {
      await restarted.stop();
    }
  });

  it("keeps separate grain keys independent", async () => {
    const storage = new MemoryGrainStorage();
    const silo = buildSilo(storage);
    await silo.start();
    try {
      await silo.getGrain(IAccount, "x").deposit(10);
      await silo.getGrain(IAccount, "y").deposit(3);
      expect(await silo.getGrain(IAccount, "x").getBalance()).toBe(10);
      expect(await silo.getGrain(IAccount, "y").getBalance()).toBe(3);
    } finally {
      await silo.stop();
    }
  });
});
