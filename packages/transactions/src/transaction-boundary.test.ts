import { beforeEach, describe, expect, it } from "vitest";
import { grain } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { Silo } from "@tsva/runtime/silo";
import { TransactionalValue } from "@tsva/transactions/transactional-value";

interface Account extends GrainWithStringKey {
  deposit(amount: number): Promise<void>;
  withdraw(amount: number): Promise<void>;
  balance(): Promise<number>;
}

interface Atm extends GrainWithStringKey {
  transfer(from: string, to: string, amount: number): Promise<void>;
  fund(account: string, amount: number): Promise<void>;
}

const Account = defineGrainInterface<Account>("TxAccount", {
  options: {
    deposit: { transaction: "join" },
    withdraw: { transaction: "join" },
    balance: { readOnly: true },
  },
});

const Atm = defineGrainInterface<Atm>("TxAtm", {
  options: {
    transfer: { transaction: "create" },
    fund: { transaction: "create" },
  },
});

@grain({ name: "TxAccount" })
class AccountGrain extends Grain implements Account {
  private readonly bal = new TransactionalValue<number>(0);

  async deposit(amount: number): Promise<void> {
    this.bal.set(this.bal.get() + amount);
  }

  async withdraw(amount: number): Promise<void> {
    const current = this.bal.get();
    if (current < amount) throw new Error("overdraft");
    this.bal.set(current - amount);
  }

  async balance(): Promise<number> {
    return this.bal.get();
  }
}

@grain({ name: "TxAtm" })
class AtmGrain extends Grain implements Atm {
  async transfer(from: string, to: string, amount: number): Promise<void> {
    // Credit first, then debit: the debit may overdraft and throw, which must
    // abort the already-staged credit (no half-apply).
    await this.getGrain(Account, to).deposit(amount);
    await this.getGrain(Account, from).withdraw(amount);
  }

  async fund(account: string, amount: number): Promise<void> {
    await this.getGrain(Account, account).deposit(amount);
  }
}

describe("transaction boundaries (Slice 1)", () => {
  let silo: Silo;
  let atm: Atm;

  beforeEach(() => {
    silo = new Silo();
    silo
      .registerGrain(AccountGrain, { interfaces: [Account] })
      .registerGrain(AtmGrain, { interfaces: [Atm] });
    silo.start();
    atm = silo.getGrain(Atm, "atm");
  });

  it("commits a transfer across two grains atomically", async () => {
    await atm.fund("A", 100);

    await atm.transfer("A", "B", 30);

    expect(await silo.getGrain(Account, "A").balance()).toBe(70);
    expect(await silo.getGrain(Account, "B").balance()).toBe(30);
  });

  it("aborts both grains when a participant fails (no half-apply)", async () => {
    await atm.fund("A", 100);

    await expect(atm.transfer("A", "B", 150)).rejects.toThrow("overdraft");

    // The credit to B must have rolled back along with the failed debit.
    expect(await silo.getGrain(Account, "A").balance()).toBe(100);
    expect(await silo.getGrain(Account, "B").balance()).toBe(0);
  });

  it("rejects a join method called outside a transaction", async () => {
    await expect(silo.getGrain(Account, "C").deposit(10)).rejects.toThrow(/transaction/i);
  });
});
