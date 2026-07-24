import { metrics, type Attributes, type Counter } from "@opentelemetry/api";

/**
 * OpenTelemetry counters for pulling-agent stream delivery (`@tsva/streams`):
 * `tsva.streams.delivered` (an event handed to its subscribers),
 * `tsva.streams.failed` (an event skipped after exhausting its retry budget),
 * and `tsva.streams.agent.polls` (one queue-pulling agent poll cycle).
 * Emitted through the global OpenTelemetry meter — a no-op until the host
 * registers an SDK.
 *
 * Instruments are created lazily on first use, not at module load — see
 * `storage-metrics.ts` for why (the metrics API is not a late-binding proxy).
 */
interface Instruments {
  delivered: Counter;
  failed: Counter;
  polls: Counter;
}

let instruments: Instruments | undefined;

function instrumentsOf(): Instruments {
  if (instruments === undefined) {
    const meter = metrics.getMeter("@tsva/observability");
    instruments = {
      delivered: meter.createCounter("tsva.streams.delivered", {
        description: "Stream events delivered to subscribers",
        unit: "{event}",
      }),
      failed: meter.createCounter("tsva.streams.failed", {
        description: "Stream events skipped after exhausting the delivery retry budget",
        unit: "{event}",
      }),
      polls: meter.createCounter("tsva.streams.agent.polls", {
        description: "Queue-pulling agent poll cycles",
        unit: "{poll}",
      }),
    };
  }
  return instruments;
}

export function recordStreamDelivered(attrs: Attributes = {}): void {
  instrumentsOf().delivered.add(1, attrs);
}

export function recordStreamFailed(attrs: Attributes = {}): void {
  instrumentsOf().failed.add(1, attrs);
}

export function recordAgentPoll(attrs: Attributes = {}): void {
  instrumentsOf().polls.add(1, attrs);
}
