import { describe, expect, it } from "vitest";
import { defineGrain, useTransactionalState } from "@tsva/core/define-grain";
import { grain, transactionalState } from "@tsva/core/decorators";
import { Grain } from "@tsva/core/grain";
import { defineGrainInterface } from "@tsva/core/grain-interface";
import type { GrainWithStringKey } from "@tsva/core/key-kinds";
import { SiloAddress } from "@tsva/core/silo-address";
import type { TransactionalState } from "@tsva/core/transactional-state";
import { InProcessNetwork } from "@tsva/messaging/in-process-transport";
import { createSilo } from "@tsva/hosting/silo-builder";

interface Balance {
  cents: number;
}

interface Account extends GrainWithStringKey {
  deposit(cents: number): Promise<void>;
  withdraw(cents: number): Promise<void>;
  balance(): Promise<number>;
}

interface FnAccount extends GrainWithStringKey {
  deposit(cents: number): Promise<void>;
  balance(): Promise<number>;
}

interface Atm extends GrainWithStringKey {
  transfer(from: string, to: string, cents: number): Promise<void>;
  fund(account: string, cents: number): Promise<void>;
  fundFn(account: string, cents: number): Promise<void>;
}

const accountOptions = {
  deposit: { transaction: "join" },
  withdraw: { transaction: "join" },
  balance: { transaction: "createOrJoin", readOnly: true },
} as const;

const Account = defineGrainInterface<Account>("TxFacetAccount", { options: accountOptions });
const FnAccount = defineGrainInterface<FnAccount>("TxFacetFnAccount", {
  options: { deposit: { transaction: "join" }, balance: { transaction: "createOrJoin" } },
});
const Atm = defineGrainInterface<Atm>("TxFacetAtm", {
  options: {
    transfer: { transaction: "create" },
    fund: { transaction: "create" },
    fundFn: { transaction: "create" },
  },
});

@grain()
class AccountGrain extends Grain implements Account {
  @transactionalState("balance", { initial: (): Balance => ({ cents: 0 }) })
  private bal!: TransactionalState<Balance>;

  async deposit(cents: number): Promise<void> {
    await this.bal.performUpdate((s) => {
      s.cents += cents;
    });
  }

  async withdraw(cents: number): Promise<void> {
    await this.bal.performUpdate((s) => {
      if (s.cents < cents) throw new Error("overdraft");
      s.cents -= cents;
    });
  }

  async balance(): Promise<number> {
    return this.bal.performRead((s) => s.cents);
  }
}

// Functional counterpart exercising the `useTransactionalState` hook.
const FnAccountGrain = defineGrain<FnAccount>("TxFacetFnAccount", (ctx) => {
  const bal = useTransactionalState<Balance>(ctx, "balance", { initial: () => ({ cents: 0 }) });
  return {
    deposit: (cents: number) =>
      bal.performUpdate((s) => {
        s.cents += cents;
      }),
    balance: () => bal.performRead((s) => s.cents),
  };
});

@grain()
class AtmGrain extends Grain implements Atm {
  async transfer(from: string, to: string, cents: number): Promise<void> {
    await this.getGrain(Account, to).deposit(cents);
    await this.getGrain(Account, from).withdraw(cents);
  }

  async fund(account: string, cents: number): Promise<void> {
    await this.getGrain(Account, account).deposit(cents);
  }

  async fundFn(account: string, cents: number): Promise<void> {
    await this.getGrain(FnAccount, account).deposit(cents);
  }
}

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo() {
  return createSilo({ clusterId: "c1", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .registerGrain(AccountGrain, { interfaces: [Account] })
    .registerGrain(FnAccountGrain, { interfaces: [FnAccount] })
    .registerGrain(AtmGrain, { interfaces: [Atm] })
    .build();
}

describe("transactional-state facet end-to-end (Slice 2)", () => {
  it("commits a transfer across two grains atomically", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      const atm = silo.getGrain(Atm, "atm");
      await atm.fund("A", 100);
      await atm.transfer("A", "B", 30);
      expect(await silo.getGrain(Account, "A").balance()).toBe(70);
      expect(await silo.getGrain(Account, "B").balance()).toBe(30);
    } finally {
      await silo.stop();
    }
  });

  it("aborts both grains on a participant failure (no half-apply)", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      const atm = silo.getGrain(Atm, "atm");
      await atm.fund("A", 100);
      await expect(atm.transfer("A", "B", 150)).rejects.toThrow("overdraft");
      expect(await silo.getGrain(Account, "A").balance()).toBe(100);
      expect(await silo.getGrain(Account, "B").balance()).toBe(0);
    } finally {
      await silo.stop();
    }
  });

  it("rejects a join method called outside a transaction", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      await expect(silo.getGrain(Account, "Z").deposit(10)).rejects.toThrow(/transaction/i);
    } finally {
      await silo.stop();
    }
  });

  it("binds the facet through the functional useTransactionalState hook", async () => {
    const silo = buildSilo();
    await silo.start();
    try {
      await silo.getGrain(Atm, "atm").fundFn("F", 42);
      expect(await silo.getGrain(FnAccount, "F").balance()).toBe(42);
    } finally {
      await silo.stop();
    }
  });
});
