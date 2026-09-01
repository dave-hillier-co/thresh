import { describe, expect, it } from "vitest";
import { grain, reducerState } from "@thresh/core/decorators";
import { Grain } from "@thresh/core/grain";
import { defineGrainInterface } from "@thresh/core/grain-interface";
import type { GrainKey } from "@thresh/core/key-kinds";
import type { ReducerState } from "@thresh/core/reducer-state";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";

type AccountEvent = { kind: "deposited"; cents: number } | { kind: "withdrawn"; cents: number };
interface AccountState {
  balanceCents: number;
  transactions: number;
}

// Pure reducer: no I/O, returns new immutable state.
const reduceAccount = (state: AccountState, event: AccountEvent): AccountState =>
  event.kind === "deposited"
    ? { balanceCents: state.balanceCents + event.cents, transactions: state.transactions + 1 }
    : { balanceCents: state.balanceCents - event.cents, transactions: state.transactions + 1 };

interface IAccount extends GrainKey<string> {
  deposit(cents: number): Promise<number>;
  withdraw(cents: number): Promise<number>;
  balance(): Promise<number>;
  transactions(): Promise<number>;
}
const IAccount = defineGrainInterface<IAccount>("IAccount.reducer", {
  options: { balance: { readOnly: true }, transactions: { readOnly: true } },
});

@grain()
class AccountGrain extends Grain implements IAccount {
  @reducerState<AccountState, AccountEvent>("account", {
    initial: () => ({ balanceCents: 0, transactions: 0 }),
    reduce: reduceAccount,
  })
  private state!: ReducerState<AccountState, AccountEvent>;

  // Command handlers do the impure work (validate), then raise events.
  async deposit(cents: number): Promise<number> {
    if (cents <= 0) throw new Error("amount must be positive");
    this.state.raise({ kind: "deposited", cents });
    await this.state.write();
    return this.state.value.balanceCents;
  }

  async withdraw(cents: number): Promise<number> {
    if (cents > this.state.value.balanceCents) throw new Error("insufficient funds");
    this.state.raise({ kind: "withdrawn", cents });
    await this.state.write();
    return this.state.value.balanceCents;
  }

  async balance(): Promise<number> {
    return this.state.value.balanceCents;
  }

  async transactions(): Promise<number> {
    return this.state.value.transactions;
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(storage: MemoryGrainStorage): SiloHost {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryStorage(storage)
    .registerGrain(AccountGrain, { interfaces: [IAccount] })
    .build();
}

describe("reducer grains end-to-end", () => {
  it("folds raised events into immutable state", async () => {
    const silo = buildSilo(new MemoryGrainStorage());
    await silo.start();
    try {
      const acct = silo.getGrain(IAccount, "a");
      expect(await acct.deposit(100)).toBe(100);
      expect(await acct.deposit(50)).toBe(150);
      expect(await acct.withdraw(30)).toBe(120);
      expect(await acct.transactions()).toBe(3);
    } finally {
      await silo.stop();
    }
  });

  it("rejects an invalid command without advancing state", async () => {
    const silo = buildSilo(new MemoryGrainStorage());
    await silo.start();
    try {
      const acct = silo.getGrain(IAccount, "a");
      await acct.deposit(100);
      await expect(acct.withdraw(500)).rejects.toThrow(/insufficient/);
      await expect(acct.deposit(-1)).rejects.toThrow(/positive/);
      expect(await acct.balance()).toBe(100);
      expect(await acct.transactions()).toBe(1); // only the one successful deposit folded
    } finally {
      await silo.stop();
    }
  });

  it("persists the snapshot across a silo restart (events are transient)", async () => {
    const storage = new MemoryGrainStorage();
    const first = buildSilo(storage);
    await first.start();
    expect(await first.getGrain(IAccount, "a").deposit(100)).toBe(100);
    expect(await first.getGrain(IAccount, "a").deposit(50)).toBe(150);
    await first.stop(); // pod dies

    const restarted = buildSilo(storage); // new pod, same durable store
    await restarted.start();
    try {
      expect(await restarted.getGrain(IAccount, "a").balance()).toBe(150);
      expect(await restarted.getGrain(IAccount, "a").transactions()).toBe(2);
    } finally {
      await restarted.stop();
    }
  });
});
