import { runThermostatDemo } from "@tsva/example-thermostat/demo";

// Runnable entry point: `pnpm --filter @tsva/example-thermostat start`.
async function main(): Promise<void> {
  const result = await runThermostatDemo();
  console.log("Thermostat demo");
  console.log("  commands per update:", JSON.stringify(result.commands));
  console.log("  last status:", JSON.stringify(result.status));
  console.log("  telemetry samples:", result.telemetrySamples);
  console.log("  telemetry average tempC:", result.telemetryAverage);
  console.log("  self-test commands after reminder:", result.pendingAfterSelfCheck);
}

main().catch((err: unknown) => {
  console.error(err);
  process.exitCode = 1;
});
