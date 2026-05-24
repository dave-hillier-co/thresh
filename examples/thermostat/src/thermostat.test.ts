import { describe, expect, it } from "vitest";
import { runThermostatDemo } from "@tsva/example-thermostat/demo";

// Smoke test: runs the worked example end-to-end so it can't silently rot.
describe("thermostat example", () => {
  it("persists state, streams telemetry, and fires the self-check reminder", async () => {
    const result = await runThermostatDemo();

    // onUpdate returns heat below target, cool above it.
    expect(result.commands[0]).toEqual([{ kind: "heat" }]);
    expect(result.commands[1]).toEqual([{ kind: "cool" }]);

    // Last persisted status reflects the most recent update before the read.
    expect(result.status.tempC).toBe(23);

    // The aggregator received both telemetry samples over the stream.
    expect(result.telemetrySamples).toBe(2);
    expect(result.telemetryAverage).toBeCloseTo(20.5);

    // The durable self-check reminder fired, queuing a self-test command.
    expect(result.pendingAfterSelfCheck).toBe(1);
  });
});
