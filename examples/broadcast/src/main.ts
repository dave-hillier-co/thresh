import { runBroadcastDemo } from "@thresh/example-broadcast/demo";

// Runnable entry point: `pnpm --filter @thresh/example-broadcast start`.
async function main(): Promise<void> {
  const result = await runBroadcastDemo();
  console.log("Broadcast channels demo");
  console.log("  region monitors (alerts each received):");
  for (const [region, texts] of Object.entries(result.monitors)) {
    console.log(`    ${region}:`, JSON.stringify(texts));
  }
  console.log("  region audit logs:");
  for (const [region, entries] of Object.entries(result.audits)) {
    console.log(`    ${region}:`, JSON.stringify(entries));
  }
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
