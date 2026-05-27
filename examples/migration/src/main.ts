import { runMigrationDemo } from "@tsva/example-migration/demo";

// Runnable entry point: `pnpm --filter @tsva/example-migration start`.
async function main(): Promise<void> {
  const result = await runMigrationDemo();
  console.log("Grain migration demo");
  console.log(`  cart hosted on silo-${result.hostBefore} before the move`);
  console.log(`  cart hosted on silo-${result.hostAfter} after the move`);
  console.log("  cart contents on the new host:", JSON.stringify(result.itemsAfterMove));
  console.log(
    result.itemsAfterMove.includes("pear")
      ? "  -> the unpersisted 'pear' survived the live migration"
      : "  -> WARNING: unpersisted item was lost",
  );
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
