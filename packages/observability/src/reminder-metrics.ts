import { metrics, type Attributes, type Counter } from "@opentelemetry/api";

/**
 * OpenTelemetry counters for reminder ticks (`@tsva/reminders`):
 * `tsva.reminders.fired` (delivery to the grain succeeded) and
 * `tsva.reminders.missed` (delivery threw and was swallowed, e.g. the grain
 * activation failed). Emitted through the global OpenTelemetry meter — a
 * no-op until the host registers an SDK.
 *
 * Instruments are created lazily on first use, not at module load — see
 * `storage-metrics.ts` for why (the metrics API is not a late-binding proxy).
 */
interface Instruments {
  fired: Counter;
  missed: Counter;
}

let instruments: Instruments | undefined;

function instrumentsOf(): Instruments {
  if (instruments === undefined) {
    const meter = metrics.getMeter("@tsva/observability");
    instruments = {
      fired: meter.createCounter("tsva.reminders.fired", {
        description: "Reminder ticks successfully delivered to their grain",
        unit: "{tick}",
      }),
      missed: meter.createCounter("tsva.reminders.missed", {
        description: "Reminder ticks whose delivery failed",
        unit: "{tick}",
      }),
    };
  }
  return instruments;
}

export function recordReminderFired(attrs: Attributes = {}): void {
  instrumentsOf().fired.add(1, attrs);
}

export function recordReminderMissed(attrs: Attributes = {}): void {
  instrumentsOf().missed.add(1, attrs);
}
