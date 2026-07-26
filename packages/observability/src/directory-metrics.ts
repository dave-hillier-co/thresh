import { metrics, type Attributes, type Counter } from "@opentelemetry/api";

/**
 * OpenTelemetry counters for the grain directory (`@thresh/directory`):
 * `thresh.directory.lookups` and `thresh.directory.registrations`, each tagged
 * `thresh.directory.locality` (`"local"` when this silo owns the grain's
 * partition, `"remote"` when the call routed to a peer). Distinct from the
 * location-cache hit/miss gauge in `runtime-metrics.ts`, which measures the
 * caller-side cache in front of the directory, not the directory itself.
 * Emitted through the global OpenTelemetry meter — a no-op until the host
 * registers an SDK.
 *
 * Instruments are created lazily on first use, not at module load — see
 * `storage-metrics.ts` for why (the metrics API is not a late-binding proxy).
 */
interface Instruments {
  lookups: Counter;
  registrations: Counter;
}

let instruments: Instruments | undefined;

function instrumentsOf(): Instruments {
  if (instruments === undefined) {
    const meter = metrics.getMeter("@thresh/observability");
    instruments = {
      lookups: meter.createCounter("thresh.directory.lookups", {
        description: "Grain directory lookups",
        unit: "{lookup}",
      }),
      registrations: meter.createCounter("thresh.directory.registrations", {
        description: "Grain directory registrations",
        unit: "{registration}",
      }),
    };
  }
  return instruments;
}

export type DirectoryLocality = "local" | "remote";

export function recordDirectoryLookup(locality: DirectoryLocality, attrs: Attributes = {}): void {
  instrumentsOf().lookups.add(1, { "thresh.directory.locality": locality, ...attrs });
}

export function recordDirectoryRegistration(
  locality: DirectoryLocality,
  attrs: Attributes = {},
): void {
  instrumentsOf().registrations.add(1, { "thresh.directory.locality": locality, ...attrs });
}
