import { runChatDemo } from "@thresh/example-chat/demo";

// Runnable entry point: `pnpm --filter @thresh/example-chat start`.
async function main(): Promise<void> {
  const result = await runChatDemo();
  console.log("Chat demo");
  console.log("  fan-out (messages each member received):");
  for (const [name, messages] of Object.entries(result.fanOut)) {
    console.log(`    ${name}:`, JSON.stringify(messages.map((m) => `${m.from}: ${m.text}`)));
  }
  console.log(
    "  bob resumed after idle deactivation:",
    JSON.stringify(result.bobResumedWhileAway.map((m) => `${m.from}: ${m.text}`)),
  );
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
