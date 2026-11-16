import { metrics, type Attributes, type Counter, type Histogram } from "@opentelemetry/api";

/**
 * OpenTelemetry instruments for the transport layer (`@thresh/messaging`):
 * `thresh.messaging.sent`/`thresh.messaging.received` counters for messages
 * crossing a connection, and `thresh.messaging.queue.latency` — the time an
 * outbound message waited for its connection to be acquired (dialed) before
 * it could be sent, i.e. Orleans' outbound-queue delay. Emitted through the
 * global OpenTelemetry meter — a no-op until the host registers an SDK.
 *
 * Instruments are created lazily on first use, not at module load — see
 * `storage-metrics.ts` for why (the metrics API is not a late-binding proxy).
 */
interface Instruments {
  sent: Counter;
  received: Counter;
  queueLatency: Histogram;
  decodeFailed: Counter;
  handlerFailed: Counter;
  misrouted: Counter;
  replyFailed: Counter;
}

let instruments: Instruments | undefined;

function instrumentsOf(): Instruments {
  if (instruments === undefined) {
    const meter = metrics.getMeter("@thresh/observability");
    instruments = {
      sent: meter.createCounter("thresh.messaging.sent", {
        description: "Messages sent over a connection",
        unit: "{message}",
      }),
      received: meter.createCounter("thresh.messaging.received", {
        description: "Messages received over a connection",
        unit: "{message}",
      }),
      queueLatency: meter.createHistogram("thresh.messaging.queue.latency", {
        description:
          "Time an outbound message waited to acquire its connection before it could be sent",
        unit: "ms",
      }),
      decodeFailed: meter.createCounter("thresh.messaging.decode.failed", {
        description:
          "Inbound frames that could not be decoded; the connection that carried one is closed",
        unit: "{frame}",
      }),
      handlerFailed: meter.createCounter("thresh.messaging.handler.failed", {
        description: "Inbound messages whose handler threw or rejected",
        unit: "{message}",
      }),
      misrouted: meter.createCounter("thresh.messaging.response.misrouted", {
        description:
          "Responses naming an outstanding call that arrived on another connection or carried another process's epoch; they complete nothing",
        unit: "{message}",
      }),
      replyFailed: meter.createCounter("thresh.messaging.reply.failed", {
        description:
          "Replies that could not be sent down the connection their request arrived on; the request had already run",
        unit: "{message}",
      }),
    };
  }
  return instruments;
}

export function recordMessageSent(attrs: Attributes = {}): void {
  instrumentsOf().sent.add(1, attrs);
}

export function recordMessageReceived(attrs: Attributes = {}): void {
  instrumentsOf().received.add(1, attrs);
}

export function recordQueueLatency(ms: number, attrs: Attributes = {}): void {
  instrumentsOf().queueLatency.record(ms, attrs);
}

export function recordDecodeFailure(attrs: Attributes = {}): void {
  instrumentsOf().decodeFailed.add(1, attrs);
}

export function recordHandlerFailure(attrs: Attributes = {}): void {
  instrumentsOf().handlerFailed.add(1, attrs);
}

export function recordMisroutedResponse(attrs: Attributes = {}): void {
  instrumentsOf().misrouted.add(1, attrs);
}

export function recordReplyFailure(attrs: Attributes = {}): void {
  instrumentsOf().replyFailed.add(1, attrs);
}
