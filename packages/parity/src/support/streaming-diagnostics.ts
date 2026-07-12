import type { StreamId } from "@tsva/core/stream";
import { onStreamingEvent, type StreamingEvent } from "@tsva/core/streaming-diagnostics";

interface PendingWait {
  matches: (event: StreamingEvent) => boolean;
  onMatch: (event: StreamingEvent) => void;
}

function sameStream(a: StreamId, b: StreamId): boolean {
  return a.provider === b.provider && a.namespace === b.namespace && a.key === b.key;
}

/**
 * Test-side counterpart of Orleans'
 * `test/TestInfrastructure/TestExtensions/Diagnostics/StreamingDiagnosticObserver.cs`
 * (a small subset: only `itemDelivered`/`streamInactive` counting, the events
 * these ported tests need). Records every `@tsva/core/streaming-diagnostics`
 * event from the moment it's created, like upstream's `Replay()`, so a
 * `waitFor*` call matches against events that already happened as well as
 * ones still to come — a test never races a `publish()`/timer whose effect
 * isn't otherwise observable from outside the cluster.
 */
export class StreamingDiagnosticObserver {
  private readonly events: StreamingEvent[] = [];
  private readonly pending = new Set<PendingWait>();
  private readonly unsubscribe: () => void;

  static create(): StreamingDiagnosticObserver {
    return new StreamingDiagnosticObserver();
  }

  private constructor() {
    this.unsubscribe = onStreamingEvent((event) => {
      this.events.push(event);
      for (const wait of [...this.pending]) {
        if (wait.matches(event)) {
          this.pending.delete(wait);
          wait.onMatch(event);
        }
      }
    });
  }

  /** Waits until at least `expectedCount` items have been delivered on `streamId`. */
  async waitForItemDeliveryCount(
    streamId: StreamId,
    expectedCount: number,
    providerName?: string,
    timeoutMs = 10_000,
  ): Promise<void> {
    const matches = (event: StreamingEvent): boolean =>
      event.kind === "itemDelivered" &&
      sameStream(event.streamId, streamId) &&
      (providerName === undefined || event.providerName === providerName);

    const countSoFar = () => this.events.filter(matches).length;
    if (countSoFar() >= expectedCount) return;

    await new Promise<void>((resolve, reject) => {
      let seen = countSoFar();
      const wait: PendingWait = {
        matches,
        onMatch: () => {
          seen += 1;
          if (seen >= expectedCount) {
            clearTimeout(timer);
            resolve();
          } else {
            // Still short: keep waiting for the next matching event.
            this.pending.add(wait);
          }
        },
      };
      const timer = setTimeout(() => {
        this.pending.delete(wait);
        reject(
          new Error(
            `timed out waiting for ${expectedCount} item delivery/deliveries on stream ` +
              `${streamId.provider}/${streamId.namespace}/${streamId.key} (saw ${countSoFar()})`,
          ),
        );
      }, timeoutMs);
      this.pending.add(wait);
    });
  }

  /** Waits for `streamId` to be reported inactive, and returns that event. */
  async waitForStreamInactive(
    streamId: StreamId,
    providerName?: string,
    timeoutMs = 10_000,
  ): Promise<StreamingEvent> {
    const matches = (event: StreamingEvent): boolean =>
      event.kind === "streamInactive" &&
      sameStream(event.streamId, streamId) &&
      (providerName === undefined || event.providerName === providerName);

    const already = this.events.find(matches);
    if (already !== undefined) return already;

    return new Promise<StreamingEvent>((resolve, reject) => {
      const wait: PendingWait = {
        matches,
        onMatch: (event) => {
          clearTimeout(timer);
          resolve(event);
        },
      };
      const timer = setTimeout(() => {
        this.pending.delete(wait);
        reject(
          new Error(
            `timed out waiting for stream ${streamId.provider}/${streamId.namespace}/${streamId.key} ` +
              `to be reported inactive`,
          ),
        );
      }, timeoutMs);
      this.pending.add(wait);
    });
  }

  dispose(): void {
    this.unsubscribe();
    this.pending.clear();
  }
}
