import {
  BROADCAST_CHANNEL_OBSERVER,
  type BroadcastChannelHandler,
} from "@thresh/core/broadcast-channel";
import { defineGrain } from "@thresh/core/define-grain";
import { type Alert } from "@thresh/example-broadcast/types";

/**
 * A per-region dashboard, implicitly subscribed to the `alerts` namespace. A
 * monitor with key `R` receives every alert published to `(alerts, R)` — the
 * publish reactivates it if idle and runs the `BROADCAST_CHANNEL_OBSERVER`
 * handler as a turn, with no explicit subscribe. It accumulates what it has seen
 * since this activation.
 */
export const RegionMonitorGrain = defineGrain(
  "RegionMonitor",
  () => {
    const received: Alert[] = [];
    return {
      alerts: async () => received,
      [BROADCAST_CHANNEL_OBSERVER]: (): BroadcastChannelHandler<unknown> => ({
        // Mutating grain state with no lock is safe: this runs as a turn.
        onPublished: (item) => void received.push(item as Alert),
      }),
    };
  },
  { implicitChannelSubscriptions: ["alerts"] },
);
