import { runGreeterDemo } from "@tsva/example-greeter/demo";

// Runnable entry point: `pnpm --filter @tsva/example-greeter start`.
async function main(): Promise<void> {
  const result = await runGreeterDemo();
  console.log("Greeter demo");
  console.log("  first greeting:", result.firstGreeting);
  console.log("  count after 3 concurrent greets:", result.countAfterConcurrent);
  console.log("  deactivated when idle:", result.deactivatedWhenIdle);
  console.log("  count after reactivation:", result.countAfterReactivation);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
