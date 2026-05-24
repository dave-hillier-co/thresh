import { runBankDemo } from "@tsva/example-bank/demo";

const dollars = (cents: number) => `$${(cents / 100).toFixed(2)}`;

// Runnable entry point: `pnpm --filter @tsva/example-bank start`.
async function main(): Promise<void> {
  const { alice, bob, aliceAfterRestart } = await runBankDemo();
  console.log("Bank demo (reducer grains)");
  console.log(
    `  alice: ${dollars(alice.balanceCents)} (${alice.deposits} deposits, ${alice.withdrawals} withdrawals)`,
  );
  console.log(
    `  bob:   ${dollars(bob.balanceCents)} (${bob.deposits} deposits, ${bob.withdrawals} withdrawals)`,
  );
  console.log(`  alice after silo restart: ${dollars(aliceAfterRestart.balanceCents)} (snapshot)`);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
