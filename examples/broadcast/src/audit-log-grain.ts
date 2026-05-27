import {
  BROADCAST_CHANNEL_OBSERVER,
  type BroadcastChannelHandler,
} from "@tsva/core/broadcast-channel";
import { defineGrain } from "@tsva/core/define-grain";
import { type Alert, type IAuditLog } from "@tsva/example-broadcast/interfaces";

/**
 * A second grain type also implicitly subscribed to the `alerts` namespace. One
 * publish to `(alerts, R)` fans out to *every* subscribed type keyed by `R` — so
 * the region `R` audit log records the same alert the region `R` monitor sees,
 * demonstrating multi-type fan-out on a single channel.
 */
export const AuditLogGrain = defineGrain<IAuditLog>(
  "AuditLog",
  () => {
    const log: string[] = [];
    return {
      entries: async () => log,
      [BROADCAST_CHANNEL_OBSERVER]: (): BroadcastChannelHandler<unknown> => ({
        onPublished: (item) => {
          const alert = item as Alert;
          log.push(`[${alert.region}] ${alert.text}`);
        },
      }),
    };
  },
  { implicitChannelSubscriptions: ["alerts"] },
);
