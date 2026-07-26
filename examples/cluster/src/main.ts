import { runClusterDemo } from "@thresh/example-cluster/demo";

// Runnable entry point: `pnpm --filter @thresh/example-cluster start`.
async function main(): Promise<void> {
  const result = await runClusterDemo();
  console.log("WebSocket cluster demo (3 silos)");
  console.log(`  leaderboard activated on: ${result.hostSilo}`);
  console.log("  board (recorded from all 3 silos):");
  for (const { player, score } of result.top) console.log(`    ${player}: ${score}`);
  console.log(`  ${result.hostSilo} died -> reactivated on: ${result.afterFailover.newHostSilo}`);
  console.log("  board after failover (fresh activation):");
  for (const { player, score } of result.afterFailover.top) console.log(`    ${player}: ${score}`);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
