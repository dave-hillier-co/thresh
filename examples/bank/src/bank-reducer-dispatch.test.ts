import { describe, expect, it } from "vitest";
import { SiloAddress } from "@thresh/core/silo-address";
import { InProcessNetwork } from "@thresh/messaging/in-process-transport";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { createSilo } from "@thresh/hosting/silo-builder";
import type { SiloHost } from "@thresh/hosting/silo-host";
import { Account } from "@thresh/example-bank/account-reducer-grain";

const local = new SiloAddress("silo-0", "uid-0", "silo-0:11111");

function buildSilo(storage: MemoryGrainStorage): SiloHost {
  return createSilo({ clusterId: "bank-reducer", local })
    .useStaticMembership([local])
    .useInProcessTransport(new InProcessNetwork())
    .useMemoryStorage(storage)
    .registerGrain(Account.grain, { interfaces: [Account] }) // one ctor, one fixed interface
    .build();
}

describe("single-dispatch reducer grain (defineReducerGrain)", () => {
  it("routes every command through dispatch(action), runs the transfer effect, and the snapshot survives a restart", async () => {
    const storage = new MemoryGrainStorage();
    const silo = buildSilo(storage);
    await silo.start();
    let alice: unknown;
    let bob: unknown;
    try {
      await silo.getGrain(Account, "alice").dispatch({ type: "deposit", cents: 10_000 });
      await silo.getGrain(Account, "alice").dispatch({ type: "deposit", cents: 5_000 });
      await silo.getGrain(Account, "bob").dispatch({ type: "deposit", cents: 2_000 });
      // The transfer's credit leg is an effect the runtime runs against bob's grain.
      await silo
        .getGrain(Account, "alice")
        .dispatch({ type: "transferTo", to: "bob", cents: 3_000 });
      alice = await silo.getGrain(Account, "alice").query(); // read-only, no write
      bob = await silo.getGrain(Account, "bob").query();
    } finally {
      await silo.stop();
    }
    // Identical outcomes to the class and multi-method functional grains.
    expect(alice).toEqual({ balanceCents: 12_000, deposits: 2, withdrawals: 1 });
    expect(bob).toEqual({ balanceCents: 5_000, deposits: 2, withdrawals: 0 });

    const restarted = buildSilo(storage);
    await restarted.start();
    try {
      expect(await restarted.getGrain(Account, "alice").query()).toEqual(alice);
    } finally {
      await restarted.stop();
    }
  });

  it("rejects an overdraft from the pure reducer without advancing state", async () => {
    const silo = buildSilo(new MemoryGrainStorage());
    await silo.start();
    try {
      const acct = silo.getGrain(Account, "x");
      await acct.dispatch({ type: "deposit", cents: 1_000 });
      await expect(acct.dispatch({ type: "withdraw", cents: 5_000 })).rejects.toThrow(
        /insufficient/,
      );
      expect(await acct.query()).toEqual({ balanceCents: 1_000, deposits: 1, withdrawals: 0 });
    } finally {
      await silo.stop();
    }
  });
});
