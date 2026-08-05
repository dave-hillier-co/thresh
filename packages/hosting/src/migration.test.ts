import { describe, expect, it } from "vitest";
import { grain, persistentState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { GrainId } from "@thresh/core/grain-id";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { PersistentState } from "@thresh/core/persistent-state";
import { SiloAddress } from "@thresh/core/silo-address";
import { FakeTimeProvider } from "@thresh/core/test-support/fake-time-provider";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { createSilo } from "@thresh/hosting/silo-builder";

interface BalanceState {
  cents: number;
}

interface IAccount extends GrainKey<string> {
  deposit(cents: number): Promise<number>;
  pendingDeposit(cents: number): Promise<number>;
  getBalance(): Promise<number>;
  scheduleMigration(target: SiloAddress): Promise<void>;
}
const IAccount = defineGrainInterface<IAccount>("IAccount.migration", {
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

  /** Mutate in memory WITHOUT persisting — survives only if migration carries it. */
  async pendingDeposit(cents: number): Promise<number> {
    this.balance.value.cents += cents;
    return this.balance.value.cents;
  }

  async getBalance(): Promise<number> {
    return this.balance.value.cents;
  }

  async scheduleMigration(target: SiloAddress): Promise<void> {
    this.runtime.migrateOnIdle(target);
  }
}

const silo = (n: number) => new SiloAddress(`silo-${n}`, `uid-${n}`, `silo-${n}:11111`);
const accountId = new GrainId("Account", "acc-1");

function buildSilo(
  local: SiloAddress,
  network: InProcessNetwork,
  storage: MemoryGrainStorage,
  time: FakeTimeProvider,
) {
  return createSilo({
    clusterId: "c-migration",
    local,
    time,
    collectionAgeSeconds: 30,
    collectionIntervalSeconds: 10,
    random: () => 0,
  })
    .useStaticMembership([silo(0), silo(1)])
    .useInProcessTransport(network)
    .useMemoryStorage(storage)
    .registerGrain(AccountGrain, { interfaces: [IAccount] })
    .build();
}

const flush = () => new Promise((r) => setTimeout(r, 0));
async function settleUntil(pred: () => boolean, max = 200): Promise<void> {
  for (let i = 0; i < max && !pred(); i++) await flush();
  await flush();
}

describe("persistent-state migration (hosting)", () => {
  it("migrates a grain to another silo carrying even its unflushed persistent state", async () => {
    const network = new InProcessNetwork();
    const storage = new MemoryGrainStorage();
    const time = new FakeTimeProvider();
    const node0 = buildSilo(silo(0), network, storage, time);
    const node1 = buildSilo(silo(1), network, storage, time);
    await node0.start();
    await node1.start();
    try {
      // random->0 places the account on silo-0. Persist 100, then add 50 in memory only.
      expect(await node0.getGrain(IAccount, "acc-1").deposit(100)).toBe(100);
      expect(await node0.getGrain(IAccount, "acc-1").pendingDeposit(50)).toBe(150);
      expect(node0.isActive(accountId)).toBe(true);

      await node0.getGrain(IAccount, "acc-1").scheduleMigration(silo(1));

      time.advance(31_000);
      await settleUntil(() => node1.isActive(accountId));

      expect(node0.isActive(accountId)).toBe(false);
      expect(node1.isActive(accountId)).toBe(true);

      // The unflushed 150 survives: it was carried in the bag, not re-read from
      // storage (which still holds 100). This proves rehydrate skips the read.
      expect(await node1.getGrain(IAccount, "acc-1").getBalance()).toBe(150);
    } finally {
      await node0.stop();
      await node1.stop();
    }
  });
});
