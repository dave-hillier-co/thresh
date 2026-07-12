import { randomUUID } from "node:crypto";
import type { QueueEntry } from "@tsva/streams/redis-stream-queue";
import type { PullableQueue } from "@tsva/streams/queue-pulling-agent";

/**
 * A synthetic event, mirroring Orleans' `GeneratedEvent`. `Report` marks the
 * last event of a generated stream so the consumer knows to flush its final
 * count (`SimpleGenerator`'s single-stream-per-queue behaviour).
 */
export interface GeneratedEvent {
  eventType: "fill" | "report";
}

export interface StreamGeneratorConfig {
  /** Namespace every generated stream is published under. */
  streamNamespace: string;
  /** Total events generated for the queue's one synthetic stream. */
  eventsInStream: number;
}

/**
 * A test-only, in-memory "queue" that synthesizes its own events instead of
 * reading a real backing store — the generator side of Orleans'
 * `GeneratorAdapterFactory`/`SimpleGenerator`. On first pull it mints one
 * random stream id (`streamNamespace/<uuid>`, matching `SimpleGenerator`'s
 * `Guid.NewGuid()` per queue) and thereafter yields exactly `eventsInStream`
 * fill/report events for that single stream. Implements `PullableQueue` so it
 * plugs into the existing `QueuePullingAgent` unchanged.
 */
export class GeneratorStreamQueue implements PullableQueue {
  private streamKey: string | undefined;
  private committed = 0;

  constructor(private config: StreamGeneratorConfig) {}

  /** The generated stream's key once minted (undefined before the first pull). */
  get generatedStreamKey(): string | undefined {
    return this.streamKey;
  }

  /**
   * Reconfigure live (Orleans' `GeneratorAdapterFactory.ExecuteCommand` /
   * `IControllable`): resets the cursor and mints a fresh stream on the next
   * pull, as if the receiver had just been created.
   */
  reconfigure(config: StreamGeneratorConfig): void {
    this.config = config;
    this.streamKey = undefined;
    this.committed = 0;
  }

  async getCursor(): Promise<number> {
    return this.committed;
  }

  async readAfter(cursor: number, count = 128): Promise<QueueEntry[]> {
    if (this.streamKey === undefined) {
      this.streamKey = `${this.config.streamNamespace}/${randomUUID()}`;
    }
    const last = Math.min(cursor + count, this.config.eventsInStream);
    const entries: QueueEntry[] = [];
    for (let token = cursor + 1; token <= last; token++) {
      const event: GeneratedEvent = {
        eventType: token === this.config.eventsInStream ? "report" : "fill",
      };
      entries.push({ token, streamKey: this.streamKey, event });
    }
    return entries;
  }

  async commit(cursor: number): Promise<void> {
    this.committed = cursor;
  }
}
