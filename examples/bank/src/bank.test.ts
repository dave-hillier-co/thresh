import { describe, expect, it } from "vitest";
import { MemoryGrainStorage } from "@thresh/persistence/memory-grain-storage";
import { buildBankSilo, runBankDemo } from "@thresh/example-bank/demo";
import { account } from "@thresh/example-bank/interfaces";

describe("bank reducer grains", () => {
  it("runs the demo end-to-end: events fold to state, transfer moves funds, snapshot survives restart", async () => {
    const { alice, bob, aliceAfterRestart } = await runBankDemo();

    // Alice: +100 +50, then -30 transferred out.
    expect(alice).toEqual({ balanceCents: 12_000, deposits: 2, withdrawals: 1 });
    // Bob: +20, then +30 received.
    expect(bob).toEqual({ balanceCents: 5_000, deposits: 2, withdrawals: 0 });
    // Only the folded snapshot is durable, and it is restored verbatim.
    expect(aliceAfterRestart).toEqual(alice);
  });

  it("rejects an overdraft without advancing state", async () => {
    const silo = buildBankSilo(new MemoryGrainStorage());
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
