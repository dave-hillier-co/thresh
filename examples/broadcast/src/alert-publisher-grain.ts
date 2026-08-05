import { defineGrain } from "@thresh/core/define-grain";
import { type Alert, type AlertPublisher } from "@thresh/example-broadcast/interfaces";

/**
 * Publishes an alert to a region's broadcast channel `(alerts, region)`. Every
 * grain type implicitly subscribed to the `alerts` namespace and keyed by that
 * region receives it — no subscription bookkeeping, no durable queue.
 */
export const AlertPublisherGrain = defineGrain<AlertPublisher>("AlertPublisher", (ctx) => ({
  raise: async (region, text) => {
    const alert: Alert = { region, text };
    await ctx.runtime
      .getBroadcastChannelProvider()
      .getChannelWriter<Alert>({ namespace: "alerts", key: region })
      .publish(alert);
  },
}));
