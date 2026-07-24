import { metrics, type Attributes, type Counter } from "@opentelemetry/api";

/**
 * OpenTelemetry counters for the grain directory (`@tsva/directory`):
 * `tsva.directory.lookups` and `tsva.directory.registrations`, each tagged
 * `tsva.directory.locality` (`"local"` when this silo owns the grain's
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
    const meter = metrics.getMeter("@tsva/observability");
    instruments = {
      lookups: meter.createCounter("tsva.directory.lookups", {
        description: "Grain directory lookups",
        unit: "{lookup}",
      }),
      registrations: meter.createCounter("tsva.directory.registrations", {
        description: "Grain directory registrations",
        unit: "{registration}",
      }),
    };
  }
  return instruments;
}

export type DirectoryLocality = "local" | "remote";

export function recordDirectoryLookup(locality: DirectoryLocality, attrs: Attributes = {}): void {
  instrumentsOf().lookups.add(1, { "tsva.directory.locality": locality, ...attrs });
}

export function recordDirectoryRegistration(
  locality: DirectoryLocality,
  attrs: Attributes = {},
): void {
  instrumentsOf().registrations.add(1, { "tsva.directory.locality": locality, ...attrs });
}
