import { defineGrain, useOnActivate } from "@thresh/core/define-grain";
import type { StreamHandler } from "@thresh/core/stream";
import { TELEMETRY, type ThermostatStatus } from "@thresh/example-thermostat/interfaces";

/**
 * Subscribes to a device's telemetry stream and keeps a rolling average. The
 * functional counterpart to the class-based `ThermostatGrain` it consumes from —
 * the two styles interoperate over the same stream (see `thermostat-grain.ts`).
 *
 * The definition is its own interface — `getGrain(FleetAggregatorGrain, key)`
 * sees exactly the `averageTemp` / `sampleCount` surface below, inferred from
 * what the factory returns.
 */
export const FleetAggregatorGrain = defineGrain(
  "FleetAggregator",
  (ctx) => {
    let sum = 0;
    let count = 0;

    const handler = (): StreamHandler<ThermostatStatus> => ({
      onNext: async (status) => {
        sum += status.tempC;
        count++;
      },
    });

    useOnActivate(async () => {
      const stream = ctx.runtime
        .getStreamProvider()
        .getStream<ThermostatStatus>(TELEMETRY, ctx.id.key);
      const existing = await stream.getSubscriptions();
      if (existing.length > 0) await existing[0]!.resume(handler());
      else await stream.subscribe(handler());
    });

    return {
      averageTemp: async (): Promise<number> => (count === 0 ? 0 : sum / count),

      sampleCount: async (): Promise<number> => count,
    };
  },
  { interfaceOptions: { averageTemp: { readOnly: true }, sampleCount: { readOnly: true } } },
);
