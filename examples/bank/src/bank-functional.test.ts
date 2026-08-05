import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { AccountGrain } from "@thresh/example-bank/account-grain-functional";
import { account } from "@thresh/example-bank/interfaces";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(storage: MemoryGrainStorage): SiloHost {
  return createSilo({ clusterId: "bank-fn", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryStorage(storage)
    .registerGrain(AccountGrain, { interfaces: [account] })
    .build();
}

describe("functional reducer grain (defineGrain)", () => {
  it("folds events to state, moves funds on transfer, and the snapshot survives a restart", async () => {
    const storage = new MemoryGrainStorage(); // durable backend, shared across the "restart"
    const silo = buildSilo(storage);
    await silo.start();
    let alice: unknown;
    let bob: unknown;
    try {
      await silo.getGrain(account, "alice").deposit(10_000);
      await silo.getGrain(account, "alice").deposit(5_000);
      await silo.getGrain(account, "bob").deposit(2_000);
      await silo.getGrain(account, "alice").transferTo("bob", 3_000);
      alice = await silo.getGrain(account, "alice").statement();
      bob = await silo.getGrain(account, "bob").statement();
    } finally {
      await silo.stop();
    }
    // Identical outcomes to the class-based grain (examples/bank/src/bank.test.ts).
    expect(alice).toEqual({ balanceCents: 12_000, deposits: 2, withdrawals: 1 });
    expect(bob).toEqual({ balanceCents: 5_000, deposits: 2, withdrawals: 0 });

    const restarted = buildSilo(storage); // new pod, same durable store
    await restarted.start();
    try {
      expect(await restarted.getGrain(account, "alice").statement()).toEqual(alice);
    } finally {
      await restarted.stop();
    }
  });

  it("rejects an overdraft without advancing state", async () => {
    const silo = buildSilo(new MemoryGrainStorage());
    await silo.start();
    try {
      const acct = silo.getGrain(account, "x");
      await acct.deposit(1_000);
      await expect(acct.withdraw(5_000)).rejects.toThrow(/insufficient/);
      expect(await acct.statement()).toEqual({ balanceCents: 1_000, deposits: 1, withdrawals: 0 });
    } finally {
      await silo.stop();
    }
  });
});
